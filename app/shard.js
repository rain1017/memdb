'use strict';

var _ = require('lodash');
var P = require('bluebird');
var Logger = require('memdb-logger');
var util = require('util');
var redis = require('redis');
var AsyncLock = require('async-lock');
var GlobalEventEmitter = require('global-events').EventEmitter;
var EventEmitter = require('events').EventEmitter;
var backends = require('./backends');
var Document = require('./document'); //jshint ignore:line
var BackendLocker = require('./backendlocker');
var Slave = require('./slave');
var utils = require('./utils');

var STATE = {
    INITED : 0,
    STARTING : 1,
    RUNNING : 2,
    STOPING : 3,
    STOPED : 4
};

// memory limit 1024MB
var DEFAULT_MEMORY_LIMIT = 1024;

// unload doc count per GC cycle
var DEFAULT_GC_COUNT = 100;

// GC check interval
var DEFAULT_GC_INTERVAL = 1000;

// Idle time before doc is unloaded, adjust to balance memory usage
var DEFAULT_IDLE_TIMEOUT = 600 * 1000;

// Persistent delay after doc has commited
var DEFAULT_PERSISTENT_DELAY = 60 * 1000;

// Timeout for readonly cache
var DEFAULT_DOC_CACHE_TIMEOUT = 60 * 1000;

// timeout for start locking backend doc
var DEFAULT_BACKEND_LOCK_TIMEOUT = 10 * 1000;
// retry interval for backend lock
var DEFAULT_BACKEND_LOCK_RETRY_INTERVAL = 100;

// timeout for locking doc
var DEFAULT_LOCK_TIMEOUT = 10 * 1000;

// heartbeat settings, must be multiple of 1000
var DEFAULT_HEARTBEAT_INTERVAL = 2 * 1000;
var DEFAULT_HEARTBEAT_TIMEOUT = 5 * 1000;

/**
 * opts.shardId (required) - shard id
 * opts.locking - {host : '127.0.0.1', port : 6379} // Global Locking Redis
 * opts.event - {host : '127.0.0.1', port : 6379} //Global Event Redis
 * opts.backend - // Global backend storage
 *  {
 *      engine : 'mongodb',
 *      url : 'mongodb://localhost',
 *      options : {...},
 *  }
 *  or
 *  {
 *      engine : 'redis',
 *      host : '127.0.0.1',
 *      port : 6379,
 *  }
 * opts.slave - {host : '127.0.0.1', port : 6379} (redis slave for data replication)
 *
 * Events:
 * updateIndex:collName:connId - (docId, indexKey, oldValue, newValue)
 */
var Shard = function(opts){
    EventEmitter.call(this);

    opts = opts || {};
    var self = this;

    this._id = opts.shardId;
    if(!this._id){
        throw new Error('You must specify shardId');
    }

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this._id);

    opts.locking = opts.locking || {};
    opts.event = opts.event || {};

    this.config = {
        locking : {
            host : opts.locking.host || '127.0.0.1',
            port : opts.locking.port || 6379,
            db : opts.locking.db || 0,
            options : opts.locking.options || {},
        },
        event : {
            host : opts.event.host || '127.0.0.1',
            port : opts.event.port || 6379,
            db : opts.event.db || 0,
            options : opts.event.options || {},
        },
        backend : opts.backend || {},
        slave : opts.slave || {},

        persistentDelay : opts.persistentDelay || DEFAULT_PERSISTENT_DELAY,
        heartbeatInterval : opts.heartbeatInterval || DEFAULT_HEARTBEAT_INTERVAL,
        heartbeatTimeout : opts.heartbeatTimeout || DEFAULT_HEARTBEAT_TIMEOUT,
        docCacheTimeout : opts.docCacheTimeout || DEFAULT_DOC_CACHE_TIMEOUT,
        backendLockTimeout : opts.backendLockTimeout || DEFAULT_BACKEND_LOCK_TIMEOUT,
        backendLockRetryInterval : opts.backendLockRetryInterval || DEFAULT_BACKEND_LOCK_RETRY_INTERVAL,
        lockTimeout : opts.lockTimeout || DEFAULT_LOCK_TIMEOUT,

        idleTimeout : opts.idleTimeout || DEFAULT_IDLE_TIMEOUT,

        memoryLimit : opts.memoryLimit || DEFAULT_MEMORY_LIMIT,
        gcCount : opts.gcCount || DEFAULT_GC_COUNT,
        gcInterval : opts.gcInterval || DEFAULT_GC_INTERVAL,

        // only for test, DO NOT disable slave in production
        disableSlave : opts.disableSlave || false,

        collections : opts.collections || {},
    };

    this.backendLocker = new BackendLocker({
                                shardId : this._id,
                                host : this.config.locking.host,
                                port : this.config.locking.port,
                                db : this.config.locking.db,
                                options : this.config.locking.options,
                                shardHeartbeatTimeout : this.config.heartbeatTimeout,
                            });

    var backendConf = this.config.backend;
    backendConf.shardId = this._id;
    this.backend = backends.create(backendConf);

    // Redis slave for data backup
    var slaveConf = this.config.slave;
    slaveConf.shardId = this._id;
    this.slave = new Slave(slaveConf);

    // Document storage {key : doc}
    this.docs = {};
    // Newly commited docs (for incremental _save)
    this.commitedKeys = {}; // {key : true}

    // Idle timeout before unload
    this.idleTimeouts = {}; // {key : timeout}

    // GC interval
    this.gcInterval = null;

    // Doc persistent timeout
    this.persistentTimeouts = {}; // {key : timeout}
    // heartbeat interval
    this.heartbeatInterval = null;

    // Lock async tasks for each key
    this.keyLock = new AsyncLock({Promise : P});
    // task Locker
    this.taskLock = new AsyncLock({Promise : P});

    // For sending messages between shards
    var pubClient = redis.createClient(this.config.event.port, this.config.event.host, this.config.event.options);
    var subClient = redis.createClient(this.config.event.port, this.config.event.host, this.config.event.options);
    pubClient.select(this.config.event.db);
    subClient.select(this.config.event.db);
    this.globalEvent = new GlobalEventEmitter({pub : pubClient, sub: subClient});

    this.logger.info('global event inited %s:%s:%s', this.config.event.host, this.config.event.port, this.config.event.db);

    // Request for unlock backend key
    this.requestingKeys = {};
    this.onRequestKey = function(key){
        try{
            self._ensureState(STATE.RUNNING);

            self.logger.debug('on request %s', key);
            if(self.requestingKeys[key]){
                return;
            }
            self.requestingKeys[key] = true;

            self.keyLock.acquire(key, function(){
                if(!self.docs[key]){
                    self.logger.warn('shard not hold the request key %s', key);
                    return self._unlockBackend(key)
                    .catch(function(e){});
                }
                return self._unload(key);
            })
            .catch(function(e){
                self.logger.error(e.stack);
            })
            .finally(function(){
                delete self.requestingKeys[key];
            });
        }
        catch(err){
            self.logger.error(err.stack);
        }
    };
    this.globalEvent.on('request:' + this._id, this.onRequestKey);

    // Cached readonly docs {key : doc} (raw doc, not document object)
    this.cachedDocs = {};

    this.state = STATE.INITED;
};

util.inherits(Shard, EventEmitter);

var proto = Shard.prototype;

proto.start = function(){
    this._ensureState(STATE.INITED);
    this.state = STATE.STARTING;

    return P.bind(this)
    .then(function(){
        return this.backendLocker.isShardAlive(this._id);
    })
    .then(function(alive){
        if(alive){
            throw new Error('Current shard is running in some other process');
        }
    })
    .then(function(){
        return this.backend.start();
    })
    .then(function(){
        if(!this.config.disableSlave){
            return this.slave.start();
        }
    })
    .then(function(){
        if(!this.config.disableSlave){
            return this._restoreFromSlave();
        }
    })
    .then(function(){
        if(this.config.heartbeatInterval > 0){
            return this._heartbeat();
        }
    })
    .then(function(){
        if(this.config.heartbeatInterval > 0){
            this.heartbeatInterval = setInterval(this._heartbeat.bind(this), this.config.heartbeatInterval);
        }
        this.gcInterval = setInterval(this.gc.bind(this), this.config.gcInterval);

        this.state = STATE.RUNNING;
        this.emit('start');
        this.logger.info('shard started');
    });
};

proto.stop = function(){
    this._ensureState(STATE.RUNNING);

    // This will prevent any further request from clients
    // All commited data will be saved, while uncommited will be rolled back
    this.state = STATE.STOPING;

    clearInterval(this.heartbeatInterval);
    clearInterval(this.gcInterval);

    this.globalEvent.removeAllListeners('request:' + this._id);
    this.globalEvent.quit();
    this.logger.info('global event closed');

    return P.bind(this)
    .then(function(){
        // Wait for running task finish
        return this.taskLock.acquire(['gc', 'flushBackend'], function(){
        });
    })
    .then(function(){
        var self = this;

        return P.mapLimit(Object.keys(this.docs), function(key){
            return self.keyLock.acquire(key, function(){
                if(self.docs[key]){
                    self.docs[key]._unlock(); //force release existing lock
                    return self._unload(key);
                }
            });
        });
    })
    .then(function(){
        if(!this.config.disableSlave){
            return this.slave.stop();
        }
    })
    .then(function(){
        return this.backend.stop();
    })
    .then(function(){
        return this.backendLocker.shardStop(this._id);
    })
    .then(function(){
        this.state = STATE.STOPED;
        this.emit('stop');
        this.logger.info('shard stoped');
    });
};

proto.find = function(connId, key, fields){
    this._ensureState(STATE.RUNNING);

    if(this.docs[key]){
        if(this.docs[key].isFree()){
            // restart idle timer if doc doesn't locked by anyone
            this._cancelIdleTimeout(key);
            this._startIdleTimeout(key);
        }
        return this.docs[key].find(connId, fields);
    }

    var self = this;
    return this.keyLock.acquire(key, function(){
        return P.try(function(){
            return self._load(key);
        })
        .then(function(){
            return self.docs[key].find(connId, fields);
        });
    })
    .then(function(doc){
        self.logger.debug('[conn:%s] find(%s, %j) => %j', connId, key, fields, doc);
        return doc;
    });
};

proto.update = function(connId, key, doc, opts){
    this._ensureState(STATE.RUNNING);

    // Since lock is called before, so doc is loaded for sure
    var ret = this._doc(key).update(connId, doc, opts);
    this.logger.debug('[conn:%s] update(%s, %j, %j) => %s', connId, key, doc, opts, ret);
    return ret;
};

proto.insert = function(connId, key, doc){
    this._ensureState(STATE.RUNNING);

    var ret = this._doc(key).insert(connId, doc);
    this.logger.debug('[conn:%s] insert(%s, %j) => %s', connId, key, doc, ret);
    return ret;
};

proto.remove = function(connId, key){
    this._ensureState(STATE.RUNNING);

    var ret = this._doc(key).remove(connId);
    this.logger.debug('[conn:%s] remove(%s) => %s', connId, key, ret);
    return ret;
};

proto.rollback = function(connId, keys){
    this._ensureState(STATE.RUNNING);

    if(!Array.isArray(keys)){
        keys = [keys];
    }

    var self = this;
    keys.forEach(function(key){
        self._doc(key).rollback(connId);
    });

    this.logger.debug('[conn:%s] rollback(%j)', connId, keys);
};

proto.lock = function(connId, key){
    this._ensureState(STATE.RUNNING);

    if(this.isLocked(connId, key)){
        return;
    }

    // New lock will be blocked even if doc is loaded while _unloading
    var self = this;
    return this.keyLock.acquire(key, function(){
        return P.try(function(){
            return self._load(key);
        })
        .then(function(){
            return self.docs[key].lock(connId);
        });
    })
    .then(function(){
        self.logger.debug('[conn:%s] lock(%s)', connId, key);
    });
};

proto.commit = function(connId, keys){
    this._ensureState(STATE.RUNNING);

    if(!Array.isArray(keys)){
        keys = [keys];
    }
    if(keys.length === 0){
        return;
    }

    var self = this;
    return P.try(function(){
        if(self.config.disableSlave){
            return;
        }
        // Sync data to slave
        var docs = {};
        keys.forEach(function(key){
            docs[key] = self._doc(key)._getChanged();
        });
        return self.slave.setMulti(docs);
    })
    .then(function(){
        // Real Commit
        keys.forEach(function(key){
            self._doc(key).commit(connId);
        });

        self.logger.debug('[conn:%s] commit(%j)', connId, keys);
    });
};

proto.isLocked = function(connId, key){
    this._ensureState(STATE.RUNNING);
    return this.docs[key] && this.docs[key].isLocked(connId);
};

/**
 * Find doc from cache (without triggering doc transfer)
 * The data may not up to date (has some legacy on change)
 * Use this when you only need readonly access and not requiring realtime data
 */
proto.findCached = function(connId, key){
    this._ensureState(STATE.RUNNING);
    if(this.cachedDocs.hasOwnProperty(key)){ //The doc can be null
        return this.cachedDocs[key];
    }

    return P.bind(this)
    .then(function(){
        if(!!this.docs[key]){
            return this.docs[key].find(connId);
        }
        else{
            var res = this._resolveKey(key);
            return this.backend.get(res.name, res.id);
        }
    })
    .then(function(doc){
        if(!this.cachedDocs.hasOwnProperty(key)){
            this.cachedDocs[key] = doc;
            var self = this;
            setTimeout(function(){
                delete self.cachedDocs[key];
                self.logger.debug('remove cached doc %s', key);
            }, this.config.docCacheTimeout);
        }
        this.logger.debug('[conn:%s] findCached(%s) => %j', connId, key, doc);
        return doc;
    });
};

/**
 * Lock backend and load doc from backend to memory
 * Not concurrency safe and must called with lock
 */
proto._load = function(key){
    if(this.docs[key]){
        return;
    }

    this.logger.debug('start load %s', key);

    var doc = null;
    // Not using keyLock here since load is always called in other task
    return P.bind(this)
    .then(function(){
        // get backend lock
        return this._lockBackend(key);
    })
    .then(function(){
        var res = this._resolveKey(key);
        return this.backend.get(res.name, res.id);
    })
    .then(function(ret){
        doc = this._createDoc(key, ret);
        if(!this.config.disableSlave){
            // Sync data to slave
            return this.slave.set(key, ret);
        }
    })
    .then(function(){
        this._addDoc(key, doc);

        this.logger.info('loaded %s', key);
    });
};

proto._addDoc = function(key, doc){
    var self = this;

    doc.on('commit', function(){
        // Mark newly commited docs
        self.commitedKeys[key] = true;

        if(!self.persistentTimeouts.hasOwnProperty(key)){
            self.persistentTimeouts[key] = setTimeout(function(){
                delete self.persistentTimeouts[key];
                return self.keyLock.acquire(key, function(){
                    return self.persistent(key);
                })
                .catch(function(err){
                    self.logger.error(err.stack);
                });
            }, self.config.persistentDelay);
        }
    });

    this._startIdleTimeout(key);

    doc.on('lock', function(){
        self._cancelIdleTimeout(key);
    });

    doc.on('unlock', function(){
        self._startIdleTimeout(key);
    });

    var res = this._resolveKey(key);
    doc.on('updateIndex', function(connId, indexKey, oldValue, newValue){
        self.emit('updateIndex:' + res.name + ':' + connId, res.id, indexKey, oldValue, newValue);
    });

    // Loaded at this instant
    this.docs[key] = doc;
};

proto._startIdleTimeout = function(key){
    var self = this;
    this.idleTimeouts[key] = setTimeout(function(){
        return self.keyLock.acquire(key, function(){
            if(self.docs[key]){
                self.logger.debug('%s idle timed out, will unload', key);
                return self._unload(key);
            }
        });
    }, this.config.idleTimeout);
};

proto._cancelIdleTimeout = function(key){
    clearTimeout(this.idleTimeouts[key]);
    delete this.idleTimeouts[key];
};

/**
 * Not concurrency safe and must called with lock
 */
proto._unload = function(key){
    if(!this.docs[key]){
        return;
    }

    this.logger.debug('start unload %s', key);

    var doc = this.docs[key];

    return P.bind(this)
    .then(function(){
        // Wait all existing lock release
        return doc._waitUnlock();
    })
    .then(function(){
        // The doc is read only now

        // Remove persistent timeout
        if(this.persistentTimeouts.hasOwnProperty(key)){
            clearTimeout(this.persistentTimeouts[key]);
            delete this.persistentTimeouts[key];
        }

        this._cancelIdleTimeout(key);

        // Persistent immediately
        return this.persistent(key);
    })
    .then(function(){
        if(!this.config.disableSlave){
            // sync data to slave
            return this.slave.del(key);
        }
    })
    .then(function(){
        doc.removeAllListeners('commit');
        doc.removeAllListeners('updateIndex');

        // _unloaded at this instant
        delete this.docs[key];

        return P.bind(this)
        .then(function(){
            // Release backend lock
            return this._unlockBackend(key);
        })
        .then(function(){
            this.logger.info('unloaded %s', key);
        })
        .delay(this.config.backendLockRetryInterval); // Can't load again immediately, prevent 'locking hungry' in other shards
    });
};

proto._lockBackend = function(key){
    return P.bind(this)
    .then(function(){
        return this.backendLocker.tryLock(key, this._id);
    })
    .then(function(success){
        if(success){
            return;
        }
        // Wait and try
        var self = this;
        var startTick = Date.now();
        var tryLock = function(wait){
            return P.bind(self)
            .then(function(){
                return this.backendLocker.getHolderId(key);
            })
            .then(function(shardId){
                if(shardId === null){
                    // unlocked
                    return;
                }
                // request the holder for the key
                // Emit request key event
                this.globalEvent.emit('request:' + shardId, key);
                this.logger.trace('request shard[%s] for key %s', shardId, key);
            })
            .delay(wait / 2 + _.random(wait))
            .then(function(){
                return this.backendLocker.tryLock(key, this._id);
            })
            .then(function(success){
                if(success){
                    return;
                }

                if(Date.now() - startTick >= this.config.backendLockTimeout){
                    throw new Error('lock backend doc ' + key + ' timed out');
                }
                return tryLock(wait);
            });
        };
        return tryLock(this.config.backendLockRetryInterval);
    });
};

proto._unlockBackend = function(key){
    return this.backendLocker.unlock(key);
};

/**
 * Persistent one doc (if changed) to backend
 */
proto.persistent = function(key){
    if(!this.commitedKeys.hasOwnProperty(key)){
        return;
    }

    // Get reference to commited doc obj.
    // actually doc.commited is immutable
    var doc = this._doc(key)._getCommited();
    // Doc may changed again during persistent, so delete the flag now.
    delete this.commitedKeys[key];

    return P.bind(this)
    .then(function(){
        var res = this._resolveKey(key);
        return this.backend.set(res.name, res.id, doc);
    })
    .then(function(){
        this.logger.debug('persistented %s', key);
    }, function(e){
        // Persistent failed, reset the changed flag
        this.commitedKeys[key] = true;
        // rethrow
        throw e;
    });
};

// Flush changes to backend storage
proto.flushBackend = function(connId){
    this._ensureState(STATE.RUNNING);
    var self = this;

    return this.taskLock.acquire('flushBackend', function(){
        return P.mapLimit(Object.keys(self.commitedKeys), function(key){
            return self.keyLock.acquire(key, function(){
                return self.persistent(key);
            });
        });
    })
    .then(function(){
        self.logger.warn('[conn:%s] flushed Backend', connId);
    });
};

// Garbage collection
proto.gc = function(){
    if(this.state !== STATE.RUNNING){
        return;
    }
    if(this.taskLock.isBusy('gc')){
        return;
    }

    var self = this;
    return this.taskLock.acquire('gc', function(){
        var usage = process.memoryUsage();
        var memSize = usage.heapUsed;

        if(memSize < self.config.memoryLimit * 1024 * 1024){
            // Memory not reach limit, no need to gc
            return;
        }

        self.logger.warn('Start GC. Memory usage is too high, please reduce idleTimeout. %j', usage);

        var startTick = Date.now();

        // remove some doc
        var keys = [], count = 0;
        for(var key in self.docs){
            keys.push(key);
            count++;
            if(count >= self.config.gcCount){
                break;
            }
        }

        return P.mapLimit(keys, function(key){
            return self.keyLock.acquire(key, function(){
                return self._unload(key);
            });
        })
        .then(function(){
            self.logger.warn('Finish GC in %s ms. %s docs have been unloaded.', Date.now() - startTick, keys.length);
        });
    })
    .then(function(){
        process.nextTick(self.gc.bind(self));
    })
    .catch(function(e){
        self.logger.error(e.stack);
    });
};


proto._heartbeat = function(){
    return this.backendLocker.shardHeartbeat(this._id);
};

proto._restoreFromSlave = function(){
    this._ensureState(STATE.STARTING);

    return P.bind(this)
    .then(function(){
        return this.slave.getAllKeys();
    })
    .then(function(keys){
        if(keys.length === 0){
            return;
        }
        this.logger.error('Server not stopped properly, will restore data from slave');

        return P.bind(this)
        .then(function(){
            return this.slave.findMulti(keys);
        })
        .then(function(items){
            for(var key in items){
                var item = items[key];
                var doc = this._createDoc(key, item);
                this._addDoc(key, doc);
                // Set all keys as unsaved
                this.commitedKeys[key] = true;
            }
            this.logger.warn('restored %s keys from slave', keys.length);
        });
    });
};

proto._doc = function(key){
    if(!this.docs[key]){
        throw new Error(key + ' is not loaded');
    }
    return this.docs[key];
};

proto._isLoaded = function(key){
    return !!this.docs[key];
};

// key - collectionName:docId
proto._resolveKey = function(key){
    var i = key.indexOf(':');
    if(i === -1){
        throw new Error('invalid key: ' + key);
    }
    return {name : key.slice(0, i), id : key.slice(i+1)};
};


proto._createDoc = function(key, doc){
    var res = this._resolveKey(key);
    var coll = this.config.collections[res.name];
    var indexes = coll ? coll.indexes || {}: {};

    return new Document({_id : res.id, doc: doc, indexes: indexes, lockTimeout : this.config.lockTimeout});
};

proto._ensureState = function(state){
    if(this.state !== state){
        throw new Error(util.format('Server state is incorrect, expected %s, actual %s', state, this.state));
    }
};

module.exports = Shard;
