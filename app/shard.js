'use strict';

var _ = require('lodash');
var Q = require('q');
var util = require('util');
var redis = require('redis');
var uuid = require('node-uuid');
var AsyncLock = require('async-lock');
var GlobalEventEmitter = require('global-events').EventEmitter;
var EventEmitter = require('events').EventEmitter;
var backends = require('./backends');
var Document = require('./document'); //jshint ignore:line
var BackendLocker = require('./backendlocker');
var Slave = require('./slave');
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

var STATE = {
	INITED : 0,
	STARTING : 1,
	RUNNING : 2,
	STOPING : 3,
	STOPED : 4
};

// Max pending tasks for each doc
var DEFAULT_MAX_PENDING_TASKS = 100;

var DEFAULT_HEARTBEAT_INTERVAL = 60 * 1000;
var DEFAULT_HEARTBEAT_TIMEOUT = 180 * 1000;

// Interval for persistent changes to backend db
var DEFAULT_PERSISTENT_INTERVAL = 60 * 1000;

// Timeout for readonly cache
var DEFAULT_DOC_CACHE_TIMEOUT = 60 * 1000;

// Unload idle doc from memory
var DEFAULT_DOC_IDLE_TIMEOUT = 1800 * 1000;

// timeout for start locking backend doc
var DEFAULT_BACKEND_LOCK_TIMEOUT = 10 * 1000;
// retry interval for backend lock
var DEFAULT_BACKEND_LOCK_RETRY_INTERVAL = 100;

// timeout for each async task for specific key
var DEFAULT_TASKLOCK_TIMEOUT = 10 * 1000;


/**
 * opts.redis - {host : '127.0.0.1', port : 6379} (for backendLocker)
 * opts.backend - (for data persistent)
 *	{
 *		engine : 'mongodb',
 *		url : 'mongodb://localhost',
 *		options : {...},
 * 	}
 *	or
 *  {
 *		engine : 'redis',
 *		host : '127.0.0.1',
 *		port : 6379,
 *	}
 * opts.slave - {host : '127.0.0.1', port : 6379} (redis slave for data replication)
 *
 * Events:
 * docUpdateUncommited:CollectionName - (connectionId, docId, field, oldValue, newValue)
 */
var Shard = function(opts){
	EventEmitter.call(this);

	opts = opts || {};
	var self = this;

	this._id = opts.shard || uuid.v4();

	opts.redis = opts.redis || {};

	this.config = {
		redis : {
			host : opts.redis.host || '127.0.0.1',
			port : opts.redis.port || 6379,
			options : opts.redis.options || {},
		},
		backend : opts.backend || {},
		slave : opts.slave || {},

		maxPendingTasks : opts.maxPendingTasks || DEFAULT_MAX_PENDING_TASKS,
		persistentInterval : opts.persistentInterval || DEFAULT_PERSISTENT_INTERVAL,
		heartbeatInterval : opts.heartbeatInterval || DEFAULT_HEARTBEAT_INTERVAL,
		docIdleTimeout : opts.docIdleTimeout || DEFAULT_DOC_IDLE_TIMEOUT,
		docCacheTimeout : opts.docCacheTimeout || DEFAULT_DOC_CACHE_TIMEOUT,
		backendLockTimeout : opts.backendLockTimeout || DEFAULT_BACKEND_LOCK_TIMEOUT,
		backendLockRetryInterval : opts.backendLockRetryInterval || DEFAULT_BACKEND_LOCK_RETRY_INTERVAL,
		heartbeatTimeout : opts.heartbeatTimeout || DEFAULT_HEARTBEAT_TIMEOUT,
		taskLockTimeout : opts.taskLockTimeout || DEFAULT_TASKLOCK_TIMEOUT,

		// only for test, DO NOT disable slave in production
		disableSlave : opts.disableSlave || false,
	};

	this.backendLocker = new BackendLocker({
								host : this.config.redis.host,
								port : this.config.redis.port,
								options : this.config.redis.options,
								shardHeartbeatTimeout : this.config.heartbeatTimeout,
							});

	this.backend = backends.create(this.config.backend);

	// Redis slave for data backup
	this.slave = new Slave(this, this.config.slave);

	// Document storage {key : doc}
	this.docs = {};
	// Newly commited docs (for incremental _save) {key : true}
	this.commitedKeys = {};
	// locker for tasks on the same doc
	this.taskLock = new AsyncLock({maxPending : this.config.maxPendingTasks, timeout : this.config.taskLockTimeout});

	this.heartbeatInterval = null;
	this.persistentInterval = null;

	// For sending messages between shards
	var pubClient = redis.createClient(this.config.redis.port, this.config.redis.host, this.config.redis.options);
	var subClient = redis.createClient(this.config.redis.port, this.config.redis.host, this.config.redis.options);
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

			self.taskLock.acquire(key, function(){
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
			.fin(function(){
				delete self.requestingKeys[key];
			});
		}
		catch(err){
			logger.error(err);
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

	var self = this;
	return Q.fcall(function(){
		logger.debug('backend start');
		return self.backend.start();
	})
	.then(function(){
		if(!self.config.disableSlave){
			return self.slave.start();
		}
	})
	.then(function(){
		if(!self.config.disableSlave){
			return self._restoreFromSlave();
		}
	})
	.then(function(){
		return self._heartbeat();
	})
	.then(function(){
		self.heartbeatInterval = setInterval(self._heartbeat.bind(self), self.config.heartbeatInterval);

		self.persistentInterval = setInterval(function(){
			return self.persistentAll();
		}, self.config.persistentInterval);

		self.state = STATE.RUNNING;
		self.emit('start');
		logger.info('shard[%s] started', self._id);
	});
};

proto.stop = function(){
	this._ensureState(STATE.RUNNING);

	// This will prevent any further request from clients
	// All commited data will be saved, while uncommited will be rolled back
	this.state = STATE.STOPING;

	clearInterval(this.heartbeatInterval);
	clearInterval(this.persistentInterval);
	this.globalEvent.removeAllListeners('request:' + this._id);
	this.globalEvent.quit();

	var self = this;
	return Q.fcall(function(){
		// Unload All
		return Q.all(Object.keys(self.docs).map(function(key){
			return self.taskLock.acquire(key, function(){
				if(self.docs[key]){
					self.docs[key]._unlock(); //force release existing lock
					return self._unload(key);
				}
			})
			.catch(function(e){
				logger.error(e);
			});
		}));
	})
	.then(function(){
		if(!self.config.disableSlave){
			return self.slave.stop();
		}
	})
	.then(function(){
		return self.backend.stop();
	})
	.then(function(){
		return self.backendLocker.shardStop(self._id);
	})
	.then(function(){
		self.state = STATE.STOPED;
		self.emit('stop');
		logger.info('shard[%s] stoped', self._id);
	});
};

proto.find = function(connectionId, key, fields){
	this._ensureState(STATE.RUNNING);
	var self = this;
	if(self.docs[key]){
		var ret = self.docs[key].find(connectionId, fields);
		logger.debug('shard[%s].find(%s, %s, %s) => %j', self._id, connectionId, key, fields, ret);
		return ret;
	}

	return self.taskLock.acquire(key, function(){
		return Q.fcall(function(){
			return self._load(key);
		}).then(function(){
			return self.docs[key].find(connectionId, fields);
		}).then(function(ret){
			logger.debug('shard[%s].find(%s, %s, %s) => %j', self._id, connectionId, key, fields, ret);
			return ret;
		});
	});
};

proto.update = function(connectionId, key, doc, opts){
	this._ensureState(STATE.RUNNING);
	// Since lock is called before, so doc is loaded for sure
	var ret = this._doc(key).update(connectionId, doc, opts);
	logger.debug('shard[%s].update(%s, %s, %j, %j) => %s', this._id, connectionId, key, doc, opts, ret);
	return ret;
};

proto.insert = function(connectionId, key, doc){
	this._ensureState(STATE.RUNNING);
	var ret = this._doc(key).insert(connectionId, doc);
	logger.debug('shard[%s].insert(%s, %s, %j) => %s', this._id, connectionId, key, doc, ret);
	return ret;
};

proto.remove = function(connectionId, key){
	this._ensureState(STATE.RUNNING);
	var ret = this._doc(key).remove(connectionId);
	logger.debug('shard[%s].remove(%s, %s) => %s', this._id, connectionId, key, ret);
	return ret;
};

proto.rollback = function(connectionId, key){
	this._ensureState(STATE.RUNNING);
	var ret = this._doc(key).rollback(connectionId);
	logger.debug('shard[%s].rollback(%s, %s) => %s', this._id, connectionId, key, ret);
	return ret;
};

proto.lock = function(connectionId, key){
	this._ensureState(STATE.RUNNING);
	var self = this;
	if(self.isLocked(connectionId, key)){
		return;
	}

	// New lock will be blocked even if doc is loaded while _unloading
	return self.taskLock.acquire(key, function(){
		return Q.fcall(function(){
			return self._load(key);
		}).then(function(){
			return self.docs[key].lock(connectionId);
		}).then(function(ret){
			logger.debug('shard[%s].lock(%s, %s) => %s', self._id, connectionId, key, ret);
			return ret;
		});
	});
};

proto.commit = function(connectionId, keys){
	this._ensureState(STATE.RUNNING);
	var self = this;
	if(!Array.isArray(keys)){
		keys = [keys];
	}
	if(keys.length === 0){
		return;
	}

	return Q.fcall(function(){
		if(self.config.disableSlave){
			return;
		}
		// Sync commited data to slave
		var changes = {};
		keys.forEach(function(key){
			changes[key] = self._doc(key).getChange(connectionId);
		});
		return self.slave.commit(changes);
	})
	.then(function(){
		// Real Commit
		keys.forEach(function(key){
			self._doc(key).commit(connectionId);
		});
		logger.debug('shard[%s].commit(%s, %j)', self._id, connectionId, keys);
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
		var ret = this.cachedDocs[key];
		logger.debug('shard[%s].findCached(%s) => %j (hit cache)', this._id, key, ret);
		return ret;
	}

	var self = this;
	return Q.fcall(function(){
		if(!!self.docs[key]){
			return self.docs[key].find(connectionId);
		}
		else{
			var res = self._resolveKey(key);
			return self.backend.get(res.name, res.id);
		}
	})
	.then(function(doc){
		if(!self.cachedDocs.hasOwnProperty(key)){
			self.cachedDocs[key] = doc;
			setTimeout(function(){
				delete self.cachedDocs[key];
				logger.trace('shard[%s] remove cached doc %s', self._id, key);
			}, self.config.docCacheTimeout);
		}
		var ret = self.cachedDocs[key];
		logger.debug('shard[%s].findCached(%s) => %j', self._id, key, ret);
		return ret;
	});
};
/**
 * Lock backend and load doc from backend to memory
 * Not concurrency safe and must called with lock
 */
proto._load = function(key){
	var self = this;
	if(self.docs[key]){
		return;
	}

	logger.debug('shard[%s] start load %s', self._id, key);

	var doc = null;
	// Not using taskLock here since load is always called in other task
	return Q.fcall(function(){
		// get backend lock
		return self._lockBackend(key);
	})
	.then(function(){
		var res = self._resolveKey(key);
		return self.backend.get(res.name, res.id);
	})
	.then(function(ret){
		var dct = ret || {};
		var exist = !!ret;
		doc = new Document({exist : exist, doc: dct});

		if(!self.config.disableSlave){
			// Sync data to slave
			return self.slave.insert(key, dct, exist);
		}
	})
	.then(function(){
		self._addDoc(key, doc);

		logger.info('shard[%s] loaded %s', self._id, key);
	});
};

proto._addDoc = function(key, doc){
	var self = this;
	var onIdleTimeout = function(){
		return self.taskLock.acquire(key, function(){
			logger.debug('%s idle timed out, will unload', key);
			return self._unload(key);
		})
		.catch(function(e){
			logger.warn(e);
		});
	};
	var idleTimeout = setTimeout(onIdleTimeout, self.config.docIdleTimeout);

	doc.on('commit', function(){
		try{
			// Mark newly commited docs
			self.commitedKeys[key] = true;

			// Reset idle timeout
			clearTimeout(idleTimeout);
			idleTimeout = setTimeout(onIdleTimeout, self.config.docIdleTimeout);
		}
		catch(err){
			logger.error(err);
		}
	});

	var res = self._resolveKey(key);
	doc.on('updateUncommited', function(connectionId, field, oldValue, newValue){
		self.emit('docUpdateUncommited:' + res.name, connectionId, res.id, field, oldValue, newValue);
	});

	// Loaded at this instant
	self.docs[key] = doc;
};

/**
 * Not concurrency safe and must called with lock
 */
proto._unload = function(key){
	var self = this;
	if(!self.docs[key]){
		return;
	}

	logger.debug('shard[%s] start unload %s', self._id, key);

	var doc = self.docs[key];

	return Q.fcall(function(){
		// lock the doc with a non-exist connectionId
		// in order to wait all existing lock release

		logger.trace('shard[%s] wait for %s commit', self._id, key);
		return doc.lock(uuid.v4());
	})
	.then(function(){
		// The doc is read only now
		logger.trace('shard[%s] wait for %s commit done', self._id, key);

		return self.persistent(key);
	})
	.then(function(){
		if(!self.config.disableSlave){
			// sync data to slave
			return self.slave.remove(key);
		}
	})
	.then(function(){
		doc.removeAllListeners('commit');
		doc.removeAllListeners('updateUncommited');

		// _unloaded at this instant
		delete self.docs[key];

		return Q.fcall(function(){
			// Release backend lock
			return self._unlockBackend(key);
		})
		.then(function(){
			logger.info('shard[%s] unloaded %s', self._id, key);
		})
		.delay(self.config.backendLockRetryInterval); // Can't load again immediately, prevent 'locking hungry' in other shards
	});
};

proto._lockBackend = function(key){
	var self = this;

	return Q.fcall(function(){
		logger.trace('shard[%s] try lock backend %s', self._id, key);
		return self.backendLocker.tryLock(key, self._id);
	}).then(function(success){
		if(success){
			logger.debug('shard[%s] locked backend %s', self._id, key);
			return;
		}

		var startTick = Date.now();

		// Wait and try
		var tryLock = function(wait){
			return Q.fcall(function(){
				return self.backendLocker.getHolderId(key);
			})
			.then(function(shardId){
				if(shardId === null){
					// unlocked
					return;
				}
				// request the holder for the key
				return Q.fcall(function(){
					// Emit request key event
					self.globalEvent.emit('request:' + shardId, key);
					logger.trace('shard[%s] request shard[%s] for key %s', self._id, shardId, key);
				});
			})
			.delay(wait / 2 + _.random(wait))
			.then(function(){
				logger.trace('shard[%s] try lock backend %s', self._id, key);
				return self.backendLocker.tryLock(key, self._id);
			})
			.then(function(success){
				if(success){
					logger.debug('shard[%s] locked backend %s', self._id, key);
					return;
				}

				if(Date.now() - startTick >= self.config.backendLockTimeout){
					throw new Error('lock backend doc ' + key + ' timed out');
				}
				return tryLock(wait);
			});
		};
		return tryLock(self.config.backendLockRetryInterval);
	});
};

proto._unlockBackend = function(key){
	var self = this;
	return Q.fcall(function(){
		return self.backendLocker.unlock(key);
	})
	.then(function(){
		logger.debug('shard[%s] unlocked backend %s', self._id, key);
	});
};

/**
 * Persistent one doc (if changed) to backend
 */
proto.persistent = function(key){
	var self = this;
	if(!self.commitedKeys.hasOwnProperty(key)){
		return;
	}
	var doc = self._doc(key).find();
	// Doc may changed again during persistent, so delete the flag now.
	delete self.commitedKeys[key];

	return Q.fcall(function(){
		return Q.fcall(function(){
			var res = self._resolveKey(key);
			return self.backend.set(res.name, res.id, doc);
		})
		.then(function(){
			logger.info('shard[%s] persistented %s', self._id, key);
		}, function(e){
			// Persistent failed, reset the changed flag
			self.commitedKeys[key] = true;
			logger.error('shard[%s] persistent %s error - %s', self._id, key, e.message);
		});
	});
};

/**
 * Persistent changed docs to backend
 */
proto.persistentAll = function(){
	var self = this;
	logger.info('shard[%s] start persistent all', self._id);
	return Q.all(Object.keys(self.commitedKeys).map(function(key){
		return self.taskLock.acquire(key, function(){
			return self.persistent(key);
		})
		.catch(function(e){
			logger.error(e);
		});
	}))
	.then(function(){
		logger.info('shard[%s] finish persistent all', self._id);
	});
};

proto._heartbeat = function(){
	var self = this;
	return Q.fcall(function(){
		return self.backendLocker.shardHeartbeat(self._id);
	});
};

proto._restoreFromSlave = function(){
	this._ensureState(STATE.STARTING);
	var self = this;

	return Q.fcall(function(){
		return self.slave.getAllKeys();
	})
	.then(function(keys){
		if(keys.length === 0){
			return;
		}
		logger.warn('Shard[%s] not stopped properly, will restore data from slave', self._id);

		return Q.fcall(function(){
			return self.slave.findMulti(keys);
		})
		.then(function(items){
			for(var key in items){
				var item = items[key];
				var doc = new Document({exist : item.exist, doc: item.fields});
				self._addDoc(key, doc);
				// Set all keys as unsaved
				self.commitedKeys[key] = true;
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

proto._ensureState = function(state){
	if(this.state !== state){
		throw new Error(util.format('shard[%s] state is incorrect, expected %s, actual %s', this._id, state, this.state));
	}
};

proto.inspect = function() {
	logger.debug('shard[%s] docs: %j', this._id, this.docs);
};

module.exports = Shard;
