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

// Max pending tasks for each doc
var MAX_PENDING_TASKS = 100;

var HEARTBEAT_INTERVAL = 30 * 1000;

// Interval bettween unloading doc and loading doc
// Prevent 'locking hungry' in other shards
var DEFAULT_UNLOAD_DELAY = 50;

// Unload idle doc from memory
var DEFAULT_DOC_IDLE_TIMEOUT = 1800 * 1000;

// timeout for locking backend doc
var DEFAULT_BACKEND_LOCK_TIMEOUT = 20 * 1000;

/**
 *
 * opts.redisConfig - {host : '127.0.0.1', port : 6379}
 * opts.backend - 'mongodb' or 'redis'
 * opts.backendConfig -
 *		{host : '127.0.0.1', port : 6379} for redis backend,
 * 		{uri : 'mongodb://localhost', options : {...}} for mongodb backend
 */
var Shard = function(opts){
	opts = opts || {};
	this._id = opts._id || uuid.v4();

	var redisConfig = opts.redisConfig || {};
	var host = redisConfig.host || '127.0.0.1';
	var port = redisConfig.port || 6379;
	this.backendLocker = new BackendLocker({host : host, port : port});

	var pubClient = redis.createClient(port, host);
	var subClient = redis.createClient(port, host);
	GlobalEventEmitter.call(this, {pub : pubClient, sub: subClient});

	this.backend = backends.create(opts.backend, opts.backendConfig);

	// Document storage {key : doc}
	this.docs = {};

	// Newly commited docs (for incremental _save) {key : true}
	this.commitedDocs = {};

	// locker for tasks on the same doc
	this.taskLock = new AsyncLock({maxPending : MAX_PENDING_TASKS, timeout : 60 * 1000});

	this.heartbeatInterval = null;
};

util.inherits(Shard, GlobalEventEmitter);

var proto = Shard.prototype;

proto.start = function(){
	var self = this;
	return Q.fcall(function(){
		return self.backend.start();
	}).then(function(){
		self.heartbeatInterval = setInterval(function(){
			self.backendlocker.shardHeartbeat(self._id);
		}, HEARTBEAT_INTERVAL);
	});
};

proto.stop = function(){
	// TODO: prevent new load and Unload all
	var self = this;
	return Q.fcall(function(){
		clearInterval(self.heartbeatInterval);
		self.end();
	}).then(function(){
		return self.backend.stop();
	});
};

proto.find = function(connectionId, key, fields){
	var self = this;
	if(self.docs[key]){
		return self.docs[key].find(connectionId, fields);
	}

	return self.taskLock.acquire(key, function(){
		return Q.fcall(function(){
			return self._load(key);
		}).then(function(){
			return self.docs[key].find(connectionId, fields);
		});
	});
};

proto.update = function(connectionId, key, doc, opts){
	// Since lock is called before, so doc is loaded for sure
	return this._doc(key).update(connectionId, doc, opts);
};

proto.insert = function(connectionId, key, doc){
	return this._doc(key).insert(connectionId, doc);
};

proto.remove = function(connectionId, key){
	return this._doc(key).remove(connectionId);
};

proto.commit = function(connectionId, key){
	return this._doc(key).commit(connectionId);
};

proto.rollback = function(connectionId, key){
	return this._doc(key).rollback(connectionId);
};

proto.lock = function(connectionId, key){
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
		});
	});
};

proto.isLocked = function(connectionId, key){
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
			return self.taskLock.acquire(key, self._unload.bind(self, key));
		};
		var idleTimeout = setTimeout(onIdleTimeout, DEFAULT_DOC_IDLE_TIMEOUT);

		doc.on('commit', function(){
			// Mark newly commited docs
			self.commitedDocs[key] = true;

			// Reset idle timeout
			clearTimeout(idleTimeout);
			idleTimeout = setTimeout(onIdleTimeout, DEFAULT_DOC_IDLE_TIMEOUT);
		});

		// Request for unlock backend
		self.on('request:' + key, function(){
			// Get task lock
			return self.taskLock.acquire(key, self._unload.bind(self, key));
		});

		// Loaded at this instant
		self.docs[key] = doc;
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

	var doc = self.docs[key];

	self.removeAllListeners('request:' + key);

	if(force){
		doc.removeAllListeners('commit');
		delete self.commitedDocs[key];
		delete self.docs[key];
		return;
	}

	return Q.fcall(function(){
		// lock the doc with a non-exist connectionId
		// in order to wait all existing lock release
		return doc.lock(uuid.v4());
	}).then(function(){
		doc.removeAllListeners('commit');

		return self._save(key);
	}).then(function(){
		delete self.commitedDocs[key];

		// _unloaded at this instant
		delete self.docs[key];

		// Release backend lock
		return self._unlockBackend(key);
	}).delay(DEFAULT_UNLOAD_DELAY); // Can't load again immediately
};

proto._lockBackend = function(key){
	var self = this;

	return Q.fcall(function(){
		return self.backendLocker.tryLock(key, self._id);
	}).then(function(success){
		if(success){
			return;
		}

		// Emit request key event
		self.emit('request:' + key);
		// Call auto unlock
		self.backendLocker.autoUnlock(key);

		// Wait and try
		var tryLock = function(wait){
			return Q() //jshint ignore:line
			.delay(wait).then(function(){
				return self.backendLocker.tryLock(key, self._id);
			}).then(function(success){
				if(success){
					return;
				}

				// Wait double time and try again
				wait = wait * 2;
				if(wait > DEFAULT_BACKEND_LOCK_TIMEOUT){
					throw new Error('lock backend doc ' + key + ' timed out');
				}
				return tryLock(wait);
			});
		};
		return tryLock(50);
	});
};

proto._unlockBackend = function(key){
	return this.backendLocker.release(key, this._id);
};

// Save commited doc to backend
proto._save = function(key){
	var self = this;
	if(!self.commitedDocs[key]){
		return;
	}

	return Q.fcall(function(){
		return self.backendLocker.isLocked(key, self._id);
	}).then(function(ret){
		if(!self.commitedDocs[key]){
			return;
		}

		if(!ret){
			// Oops! We are not holding backend lock
			// Data in memory is inconsistent, so force unload it
			self._unload(key, true);
			throw new Error('backend ' + key + ' is not locked');
		}

		// WARN: There is almost no chance that backendLock is force released
		// 		 between checking lock and successfully saved to backend
		delete self.commitedDocs[key];
		var value = self._doc(key).get();
		var res = self._resolveKey(key);
		if(value === null){
			return self.backend.del(res.name, res.id);
		}
		else{
			return self.backend.set(res.name, res.id, value);
		}
	});
};

proto._saveAll = function(){
	var self = this;
	return Q.all(Object.keys(self.commitedDocs).map(function(key){
		return Q.fcall(function(){
			return self._save(key);
		}).catch(function(e){
			logger.warn(e.stack);
		});
	}));
};

proto._doc = function(key){
	if(!this.docs[key]){
		throw new Error(key + ' is not loaded');
	}
	return this.docs[key];
};

// key - collectionName:docId
proto._resolveKey = function(key){
	var strs = key.split(':');
	return {name : strs[0], id : strs[1]};
};

module.exports = Shard;
