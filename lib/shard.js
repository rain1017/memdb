'use strict';

var Q = require('q');
var util = require('util');
var redis = require('redis');
var uuid = require('node-uuid');
var AsyncLock = require('async-lock');
var GlobalEventEmitter = require('global-events').EventEmitter;
var backends = require('./backends');
var Document = require('./document'); //jshint ignore:line
var BackendLocker = require('./backendlocker');
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

var DEFAULT_HEARTBEAT_INTERVAL = 30 * 1000;
var DEFAULT_HEARTBEAT_TIMEOUT = 180 * 1000;

// Interval for persistent changes to backend db
var DEFAULT_PERSISTENT_INTERVAL = 30 * 1000;

// Interval bettween unloading doc and loading doc
// Prevent 'locking hungry' in other shards
var DEFAULT_UNLOAD_DELAY = 50;

// Unload idle doc from memory
var DEFAULT_DOC_IDLE_TIMEOUT = 1800 * 1000;

// timeout for start locking backend doc
var DEFAULT_BACKEND_LOCK_TIMEOUT = 20 * 1000;
// timeout for holding backend doc after requested by other shards
var DEFAULT_AUTOUNLOCK_TIMEOUT = 10 * 1000;
// timeout for each async task for specific key
var DEFAULT_TASKLOCK_TIMEOUT = 60 * 1000;
// timeout for persistent task
var DEFAULT_PERSISTENTLOCK_TIMEOUT = 60 * 1000;

/**
 * opts.redisConfig - {host, port} (for backendLocker)
 * opts.backend - 'mongodb' or 'redis'
 * opts.backendConfig -
 *		{host : '127.0.0.1', port : 6379} for redis backend,
 * 		{uri : 'mongodb://localhost', options : {...}} for mongodb backend
 *
 */
var Shard = function(opts){
	opts = opts || {};
	this._id = opts._id || uuid.v4();

	var redisConfig = opts.redisConfig || {};

	this.config = {
		redisConfig : {
			host : redisConfig.host || '127.0.0.1',
			port : redisConfig.port || 6379,
		},
		backend : opts.backend || 'mongodb',
		backendConfig : opts.backendConfig || {},

		maxPendingTasks : opts.maxPendingTasks || DEFAULT_MAX_PENDING_TASKS,
		persistentInterval : opts.persistentInterval || DEFAULT_PERSISTENT_INTERVAL,
		heartbeatInterval : opts.heartbeatInterval || DEFAULT_HEARTBEAT_INTERVAL,
		unloadDelay : opts.unloadDelay || DEFAULT_UNLOAD_DELAY,
		docIdleTimeout : opts.docIdleTimeout || DEFAULT_DOC_IDLE_TIMEOUT,
		backendLockTimeout : opts.backendLockTimeout || DEFAULT_BACKEND_LOCK_TIMEOUT,
		heartbeatTimeout : opts.heartbeatTimeout || DEFAULT_HEARTBEAT_TIMEOUT,
		autoUnlockTimeout : opts.autoUnlockTimeout || DEFAULT_AUTOUNLOCK_TIMEOUT,
		taskLockTimeout : opts.taskLockTimeout || DEFAULT_TASKLOCK_TIMEOUT,
		persistentLockTimeout : opts.persistentLockTimeout || DEFAULT_PERSISTENTLOCK_TIMEOUT,
	};

	this.backendLocker = new BackendLocker({
								host : this.config.redisConfig.host,
								port : this.config.redisConfig.port,
								shardHeartbeatTimeout : this.config.heartbeatTimeout,
								autoUnlockTimeout : this.config.autoUnlockTimeout,
							});

	var pubClient = redis.createClient(this.config.redisConfig.port, this.config.redisConfig.host);
	var subClient = redis.createClient(this.config.redisConfig.port, this.config.redisConfig.host);
	GlobalEventEmitter.call(this, {pub : pubClient, sub: subClient});

	this.backend = backends.create(this.config.backend, this.config.backendConfig);

	// Document storage {key : doc}
	this.docs = {};
	// Newly commited docs (for incremental _save) {key : true}
	this.commitedDocs = {};
	// locker for tasks on the same doc
	this.taskLock = new AsyncLock({maxPending : this.config.maxPendingTasks, timeout : this.config.taskLockTimeout});
	// locker for persistent changes to backend
	this.persistentLock = new AsyncLock({timeout : this.config.persistentLockTimeout});

	this.heartbeatInterval = null;
	this.persistentInterval = null;

	this.state = STATE.INITED;
};

util.inherits(Shard, GlobalEventEmitter);

var proto = Shard.prototype;

proto.start = function(){
	this._ensureState(STATE.INITED);
	this.state = STATE.STARTING;

	var self = this;
	return Q.fcall(function(){
		return self.backend.start();
	}).then(function(){
		self.backendLocker.shardHeartbeat(self._id);

		self.heartbeatInterval = setInterval(function(){
			self.backendLocker.shardHeartbeat(self._id);
		}, self.config.heartbeatInterval);

		self.persistentInterval = setInterval(function(){
			return self.persistent();
		}, self.config.persistentInterval);

		self.state = STATE.RUNNING;
		logger.info('shard[%s] started', self._id);
	});
};

proto.stop = function(){
	this._ensureState(STATE.RUNNING);

	// This will prevent any further request from clients
	// All commited data will be saved, while uncommited will be rolled back
	this.state = STATE.STOPING;

	var self = this;
	return Q.fcall(function(){
		return self.persistent();
	})
	.then(function(){
		return self._unlockBackendAll();
	})
	.then(function(){
		clearInterval(self.heartbeatInterval);
		clearInterval(self.persistentInterval);

		return self.backend.stop();
	})
	.then(function(){
		self.end();
		return self.backendLocker.shardStop(self._id);
	})
	.then(function(){
		self.state = STATE.STOPED;
		logger.info('shard[%s] stoped', self._id);
	});
};

proto.find = function(connectionId, key, fields){
	this._ensureState(STATE.RUNNING);
	var self = this;
	if(self.docs[key]){
		var ret = self.docs[key].find(connectionId, fields);
		logger.debug('shard[%s].find(%s, %s, %s) => %s', self._id, connectionId, key, fields, util.inspect(ret));
		return ret;
	}

	return self.taskLock.acquire(key, function(){
		return Q.fcall(function(){
			return self._load(key);
		}).then(function(){
			return self.docs[key].find(connectionId, fields);
		}).then(function(ret){
			logger.debug('shard[%s].find(%s, %s, %s) => %s', self._id, connectionId, key, fields, util.inspect(ret));
			return ret;
		});
	});
};

proto.update = function(connectionId, key, doc, opts){
	this._ensureState(STATE.RUNNING);
	// Since lock is called before, so doc is loaded for sure
	var ret = this._doc(key).update(connectionId, doc, opts);
	logger.debug('shard[%s].update(%s, %s, %s, %s) => %s', this._id, connectionId, key, util.inspect(doc), opts, ret);
	return ret;
};

proto.insert = function(connectionId, key, doc){
	this._ensureState(STATE.RUNNING);
	var ret = this._doc(key).insert(connectionId, doc);
	logger.debug('shard[%s].insert(%s, %s, %s) => %s', this._id, connectionId, key, util.inspect(doc), ret);
	return ret;
};

proto.remove = function(connectionId, key){
	this._ensureState(STATE.RUNNING);
	var ret = this._doc(key).remove(connectionId);
	logger.debug('shard[%s].remove(%s, %s) => %s', this._id, connectionId, key, ret);
	return ret;
};

proto.commit = function(connectionId, key){
	this._ensureState(STATE.RUNNING);
	var ret = this._doc(key).commit(connectionId);
	logger.debug('shard[%s].commit(%s, %s) => %s', this._id, connectionId, key, ret);
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

proto.isLocked = function(connectionId, key){
	this._ensureState(STATE.RUNNING);
	return this.docs[key] && this.docs[key].isLocked(connectionId);
};

/**
 * Lock backend and load doc from backend to memory
 * This method can not be called alone
 */
proto._load = function(key){
	var self = this;
	if(self.docs[key]){
		return;
	}

	logger.debug('shard[%s] start load %s', self._id, key);

	// Not using taskLock here since load is always called in other task
	return Q.fcall(function(){
		// get backend lock
		return self._lockBackend(key);
	}).then(function(){
		var res = self._resolveKey(key);
		return self.backend.get(res.name, res.id);
	}).then(function(ret){
		if(self.docs[key]){
			return;
		}

		var doc = new Document({
								exist : !!ret,
								doc: ret || {}
							});

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
			// Mark newly commited docs
			self.commitedDocs[key] = true;

			// Reset idle timeout
			clearTimeout(idleTimeout);
			idleTimeout = setTimeout(onIdleTimeout, self.config.docIdleTimeout);
		});

		// Request for unlock backend
		self.on('request:' + key, function(){
			logger.debug('shard[%s] on request %s', self._id, key);
			// Get task lock
			return self.taskLock.acquire(key, function(){
				return self._unload(key);
			})
			.catch(function(e){
				logger.warn(e);
			});
		});

		// Loaded at this instant
		self.docs[key] = doc;
		logger.info('shard[%s] loaded %s', self._id, key);
	});
};

/**
 * This method can not be called alone
 */
proto._unload = function(key, force){
	var self = this;
	if(!self.docs[key]){
		return;
	}

	logger.debug('shard[%s] start unload %s (force=%s)', self._id, key, force);

	var doc = self.docs[key];

	self.removeAllListeners('request:' + key);

	if(force){
		doc.removeAllListeners('commit');
		delete self.commitedDocs[key];
		delete self.docs[key];
		logger.warn('shard[%s] force unloaded %s', self._id, key);
		return;
	}

	return Q.fcall(function(){
		// lock the doc with a non-exist connectionId
		// in order to wait all existing lock release
		logger.trace('shard[%s] wait for %s commit', self._id, key);
		return doc.lock(uuid.v4());
	}).then(function(){
		logger.trace('shard[%s] wait for %s commit done', self._id, key);

		doc.removeAllListeners('commit');

		// If there is running persistent call, this will block
		// unlockBackend is not allowed during persistent call
		return self.persistent();
	}).then(function(){
		// _unloaded at this instant
		delete self.docs[key];

		// Release backend lock
		return self._unlockBackend(key);
	})
	.then(function(){
		logger.info('shard[%s] unloaded %s', self._id, key);
	})
	.delay(self.config.unloadDelay); // Can't load again immediately
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

		// Call auto unlock
		var autoUnlockDeferred = self.backendLocker.autoUnlock(key);

		// Wait and try
		var tryLock = function(wait){
			return Q.fcall(function(){
				// Emit request key event
				self.emit('request:' + key);
				logger.trace('shard[%s] request for key %s', self._id, key);
			})
			.delay(wait).then(function(){
				logger.trace('shard[%s] try lock backend %s', self._id, key);
				return self.backendLocker.tryLock(key, self._id);
			})
			.then(function(success){
				if(success){
					autoUnlockDeferred.resolve();
					logger.debug('shard[%s] locked backend %s', self._id, key);
					return;
				}

				// Wait double time and try again
				wait = wait * 2;
				if(wait > self.config.backendLockTimeout){
					throw new Error('lock backend doc ' + key + ' timed out');
				}
				return tryLock(wait);
			});
		};
		return tryLock(50);
	});
};

proto._unlockBackend = function(key){
	var self = this;
	return Q.fcall(function(){
		return self.backendLocker.unlock(key, self._id);
	})
	.then(function(){
		logger.debug('shard[%s] unlocked backend %s', self._id, key);
	});
};

/**
 * Persistent changed docs to backend
 * All persistent call will run in series
 */
proto.persistent = function(){
	var self = this;
	return self.persistentLock.acquire('', function(){
		var changes = {};
		for(var key in self.commitedDocs){
			changes[key] = self._doc(key).find();
		}
		self.commitedDocs = {};

		return Q.all(Object.keys(changes).map(function(key){
			var change = changes[key];
			return Q.fcall(function(){
				return self.backendLocker.isHeldBy(key, self._id);
			})
			.then(function(ret){
				if(!ret){
					logger.warn('shard[%s] loaded %s but not holding the backendLock', self._id, key);

					// Oops! We are not holding backend lock
					// Data in memory is inconsistent, so force unload it
					self._unload(key, true);
					return;
				}

				// WARN: There is almost no chance that backendLock is force released
				// 		 between checking lock and successfully saved to backend
				var res = self._resolveKey(key);
				if(change === null){
					return self.backend.del(res.name, res.id);
				}
				else{
					return self.backend.set(res.name, res.id, change);
				}
			})
			.catch(function(e){
				logger.error('shard[%s] failed to save doc %s: %s', self._id, key, e.message);
			});
		}))
		.then(function(){
			logger.info('shard[%s] persistented %s changed docs', self._id, Object.keys(changes).length);
		});
	});
};

proto._unlockBackendAll = function(){
	var self = this;
	return Q.all(Object.keys(self.docs).map(function(key){
		return Q.fcall(function(){
			return self._unlockBackend(key);
		})
		.catch(function(e){
			logger.warn(e.message);
		});
	}))
	.then(function(){
		logger.info('shard[%s] unlocked all backend keys', self._id);
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
	var strs = key.split(':');
	return {name : strs[0], id : strs[1]};
};

proto._ensureState = function(state){
	if(this.state !== state){
		throw new Error('Shard state is incorrect');
	}
};

module.exports = Shard;
