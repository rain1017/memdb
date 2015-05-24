'use strict';

var _ = require('lodash');
var P = require('bluebird');
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
var logger = require('pomelo-logger').getLogger('memdb', __filename);

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

// heartbeat settings, must be multiple 1000
var DEFAULT_HEARTBEAT_INTERVAL =  1000;
var DEFAULT_HEARTBEAT_TIMEOUT = 3 * 1000;

/**
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
 * docUpdateIndex:CollectionName - (connectionId, docId, field, oldValue, newValue)
 */
var Shard = function(opts){
    EventEmitter.call(this);

    opts = opts || {};
    var self = this;

    this._id = opts.shard;
    if(!this._id){
        throw new Error('You must specify shard id');
    }

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
                                host : this.config.locking.host,
                                port : this.config.locking.port,
                                db : this.config.locking.db,
                                options : this.config.locking.options,
                                shardHeartbeatTimeout : this.config.heartbeatTimeout,
                            });

    this.backend = backends.create(this.config.backend);

    // Redis slave for data backup
    this.slave = new Slave(this, this.config.slave);

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

    // Request for unlock backend key
    this.requestingKeys = {};
    this.onRequestKey = function(key){
        try{
            self._ensureState(STATE.RUNNING);

            logger.debug('shard[%s] on request %s', self._id, key);
            if(self.requestingKeys[key]){
                return;
            }
            self.requestingKeys[key] = true;

            self.keyLock.acquire(key, function(){
                if(!self.docs[key]){
                    logger.warn('shard[%s] not hold the request key %s', self._id, key);
                    return self._unlockBackend(key)
                    .catch(function(e){});
                }
                return self._unload(key);
            })
            .catch(function(e){
                logger.error(e.stack);
            })
            .finally(function(){
                delete self.requestingKeys[key];
            });
        }
        catch(err){
            logger.error(err.stack);
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
            throw new Error('Shard ' + this._id + ' is running in some other process');
        }
    })
    .then(function(){
        logger.debug('backend start');
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
        logger.info('shard[%s] started', this._id);
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
        logger.info('shard[%s] stoped', this._id);
    });
};

proto.find = function(connectionId, key, fields){
    this._ensureState(STATE.RUNNING);

    if(this.docs[key]){
        if(this.docs[key].isFree()){
            // restart idle timer if doc doesn't locked by anyone
            this._cancelIdleTimeout(key);
            this._startIdleTimeout(key);
        }
        return this.docs[key].find(connectionId, fields);
    }

    var self = this;
    return this.keyLock.acquire(key, function(){

        return P.bind(self)
        .then(function(){
            return this._load(key);
        })
        .then(function(){
            return this.docs[key].find(connectionId, fields);
        });
    });
};

proto.update = function(connectionId, key, doc, opts){
    this._ensureState(STATE.RUNNING);
    // Since lock is called before, so doc is loaded for sure
    return this._doc(key).update(connectionId, doc, opts);
};

proto.insert = function(connectionId, key, doc){
    this._ensureState(STATE.RUNNING);
    return this._doc(key).insert(connectionId, doc);
};

proto.remove = function(connectionId, key){
    this._ensureState(STATE.RUNNING);
    return this._doc(key).remove(connectionId);
};

proto.rollback = function(connectionId, key){
    this._ensureState(STATE.RUNNING);
    return this._doc(key).rollback(connectionId);
};

proto.lock = function(connectionId, key){
    this._ensureState(STATE.RUNNING);
    if(this.isLocked(connectionId, key)){
        return;
    }

    // New lock will be blocked even if doc is loaded while _unloading
    var self = this;
    return this.keyLock.acquire(key, function(){
        return P.bind(self)
        .then(function(){
            return this._load(key);
        })
        .then(function(){
            return this.docs[key].lock(connectionId);
        });
    });
};

proto.commit = function(connectionId, keys){
    this._ensureState(STATE.RUNNING);

    if(!Array.isArray(keys)){
        keys = [keys];
    }
    if(keys.length === 0){
        return;
    }

    return P.bind(this)
    .then(function(){
        if(this.config.disableSlave){
            return;
        }
        // Sync data to slave
        var docs = {};
        var self = this;
        keys.forEach(function(key){
            docs[key] = self._doc(key)._getChanged();
        });
        return this.slave.setMulti(docs);
    })
    .then(function(){
        // Real Commit
        var self = this;
        keys.forEach(function(key){
            self._doc(key).commit(connectionId);
        });
    });
};

proto.isLocked = function(connectionId, key){
    this._ensureState(STATE.RUNNING);
    return this.docs[key] && this.docs[key].isLocked(connectionId);
};

/**
 * Find doc from cache (without triggering doc transfer)
 * The data may not up to date (has some legacy on change)
 * Use this when you only need readonly access and not requiring realtime data
 */
proto.findCached = function(connectionId, key){
    this._ensureState(STATE.RUNNING);
    if(this.cachedDocs.hasOwnProperty(key)){ //The doc can be null
        return this.cachedDocs[key];
    }

    return P.bind(this)
    .then(function(){
        if(!!this.docs[key]){
            return this.docs[key].find(connectionId);
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
                logger.trace('shard[%s] remove cached doc %s', self._id, key);
            }, this.config.docCacheTimeout);
        }
        return this.cachedDocs[key];
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

    logger.debug('shard[%s] start load %s', this._id, key);

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
        logger.info('shard[%s] loaded %s', this._id, key);
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
                    logger.error(err.stack);
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
    doc.on('updateIndex', function(connectionId, indexKey, oldValue, newValue){
        self.emit('docUpdateIndex:' + res.name, connectionId, res.id, indexKey, oldValue, newValue);
    });

    // Loaded at this instant
    this.docs[key] = doc;
};

proto._startIdleTimeout = function(key){
    var self = this;
    this.idleTimeouts[key] = setTimeout(function(){
        return self.keyLock.acquire(key, function(){
            if(self.docs[key]){
                logger.info('shard[%s] %s idle timed out, will unload', self._id, key);
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

    logger.debug('shard[%s] start unload %s', this._id, key);

    var doc = this.docs[key];

    return P.bind(this)
    .then(function(){
        logger.trace('shard[%s] wait for %s commit', this._id, key);

        // Wait all existing lock release
        return doc._waitUnlock();
    })
    .then(function(){
        // The doc is read only now
        logger.trace('shard[%s] wait for %s commit done', this._id, key);

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
            logger.info('shard[%s] unloaded %s', this._id, key);
        })
        .delay(this.config.backendLockRetryInterval); // Can't load again immediately, prevent 'locking hungry' in other shards
    });
};

proto._lockBackend = function(key){
    return P.bind(this)
    .then(function(){
        logger.trace('shard[%s] try lock backend %s', this._id, key);
        return this.backendLocker.tryLock(key, this._id);
    })
    .then(function(success){
        if(success){
            logger.debug('shard[%s] locked backend %s', this._id, key);
            return;
        }

        var startTick = Date.now();

        // Wait and try
        var self = this;
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
                logger.trace('shard[%s] request shard[%s] for key %s', this._id, shardId, key);
            })
            .delay(wait / 2 + _.random(wait))
            .then(function(){
                logger.trace('shard[%s] try lock backend %s', this._id, key);
                return this.backendLocker.tryLock(key, this._id);
            })
            .then(function(success){
                if(success){
                    logger.debug('shard[%s] locked backend %s', this._id, key);
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
    return P.bind(this)
    .then(function(){
        return this.backendLocker.unlock(key);
    })
    .then(function(){
        logger.debug('shard[%s] unlocked backend %s', this._id, key);
    });
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
        logger.info('shard[%s] persistented %s', this._id, key);
    }, function(e){
        // Persistent failed, reset the changed flag
        this.commitedKeys[key] = true;

        logger.error('shard[%s] persistent %s error - %s', this._id, key, e.message);
    });
};

// Flush changes to backend storage
proto.flushBackend = function(){
    this._ensureState(STATE.RUNNING);
    var self = this;

    return this.taskLock.acquire('flushBackend', function(){
        return P.mapLimit(Object.keys(self.commitedKeys), function(key){
            return self.keyLock.acquire(key, function(){
                return self.persistent(key);
            });
        });
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

        logger.warn('Start GC. Memory usage is too high, please reduce idleTimeout. %j', usage);

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
            })
            .catch(function(err){
                logger.error(err);
            });
        })
        .then(function(){
            logger.warn('Finish GC in %s ms. %s docs have been unloaded.', Date.now() - startTick, keys.length);
        });
    })
    .then(function(){
        process.nextTick(self.gc.bind(self));
    })
    .catch(function(e){
        logger.error(e);
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
        logger.warn('Shard[%s] not stopped properly, will restore data from slave', this._id);

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
            logger.warn('restored %s keys from slave', keys.length);
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
        throw new Error(util.format('shard[%s] state is incorrect, expected %s, actual %s', this._id, state, this.state));
    }
};

proto.inspect = function() {
    logger.debug('shard[%s] docs: %j', this._id, this.docs);
};

module.exports = Shard;
