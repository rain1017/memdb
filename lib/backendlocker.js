'use strict';

var Q = require('q');
var util = require('util');
var utils = require('./utils');
var redis = require('redis');
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

var DEFAULT_AUTOUNLOCK_TIMEOUT = 10 * 1000;
var DEFAULT_SHARD_HEARTBEAT_TIMEOUT = 180 * 1000;

/**
 * Lock document in backend for specific shard
 *
 * Redis format:
 * bl:docId - shardId
 *
 */
var BackendLocker = function(opts){
	opts = opts || {};
	var host = opts.host || '127.0.0.1';
	var port = opts.port || 6379;

	this.prefix = 'doc2shard:';
	this.client = redis.createClient(port, host);

	this.shardHeartbeatTimeout = opts.shardHeartbeatTimeout || DEFAULT_SHARD_HEARTBEAT_TIMEOUT;
	this.autoUnlockTimeout = opts.autoUnlockTimeout || DEFAULT_AUTOUNLOCK_TIMEOUT;
	// Locking primitive for non-atomic operation
	this.docLock = {};
};

var proto = BackendLocker.prototype;

proto.close = function(){
	this.client.end();
};

/**
 * Lock a doc, throw exception on failure
 */
proto.lock = function(docId, shardId){
	var self = this;
	return Q.nfcall(function(cb){
		self.client.setnx(self._docKey(docId), shardId, utils.normalizecb(cb));
	}).then(function(ret){
		if(ret !== 1){
			throw new Error(docId + ' already locked by others');
		}
		logger.debug('%s locked %s', shardId, docId);
	});
};

/**
 * Lock a doc, return true on success, false on failure
 */
proto.tryLock = function(docId, shardId){
	var self = this;
	return Q.fcall(function(cb){
		return self.lock(docId, shardId);
	}).then(function(){
		return true;
	}, function(err){
		return false;
	});
};

/**
 * Get lock holder shardId
 */
proto.getHolderId = function(docId){
	var self = this;
	return Q.nfcall(function(cb){
		self.client.get(self._docKey(docId), utils.normalizecb(cb));
	});
};

// Whether docId is held by shardId
proto.isHeldBy = function(docId, shardId){
	var self = this;
	return Q.fcall(function(){
		return self.getHolderId(docId);
	}).then(function(ret){
		return ret === shardId;
	});
};

// Throw execption if docId not held by shardId
proto.ensureHeldBy = function(docId, shardId){
	var self = this;
	return Q.fcall(function(){
		return self.isHeldBy(docId, shardId);
	}).then(function(ret){
		if(!ret){
			throw new Error(docId + ' is not held by ' + shardId);
		}
	});
};

/**
 * unlock a doc
 * doc must held by shardId, otherwise will throw exception
 */
proto.unlock = function(docId, shardId){
	var self = this;

	if(self.docLock[docId]){
		throw new Error('Concurrency error');
	}

	self.docLock[docId] = true;
	var commited = false;

	return Q.fcall(function(){
		return self._watchDoc(docId);
	}).then(function(){
		return self.ensureHeldBy(docId, shardId);
	}).then(function(){
		return self._unlockCommit(docId);
	}).then(function(ret){
		commited = true;
		if(ret === null){
			throw new Error('Possibly concurrency error');
		}
		logger.debug('%s unlock %s', shardId, docId);
	}).fin(function(){
		return Q.fcall(function(){
			if(!commited){
				// Must call unwatch to end 'transaction' if not commited
				return self._unwatch();
			}
		}).fin(function(){
			delete self.docLock[docId];
		});
	});
};

// Commit unlock with multi command
proto._unlockCommit = function(docId){
	var self = this;
	return Q.nfcall(function(cb){
		self.client.multi()
			.del(self._docKey(docId))
			.exec(utils.normalizecb(cb));
	});
};

// Try unlock and return whether success
proto.tryUnlock = function(docId, shardId){
	var self = this;
	return Q.fcall(function(cb){
		return self.unlock(docId, shardId);
	}).then(function(){
		return true;
	}, function(err){
		return false;
	});
};

/**
 * Force unlock a doc, without requiring holder
 */
proto.unlockForce = function(docId){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.del(self._docKey(docId), utils.normalizecb(cb));
	}).then(function(){
		logger.debug('force unlocked %s', docId);
	});
};

/**
 * Force unlock if the holder doesn't change for a period (May due to dead lock or exceptions)
 * or the holder has no heartbeat
 */
proto.autoUnlock = function(docId, timeout){
	timeout = timeout || this.autoUnlockTimeout;
	var self = this;

	if(self.docLock[docId]){
		return;
	}

	self.docLock[docId] = true;
	var commited = false;

	// Will execute in async
	Q.fcall(function(){
		return self._watchDoc(docId);
	}).then(function(){
		return self.getHolderId(docId);
	}).then(function(shardId){
		if(!shardId){
			return;
		}
		return Q.fcall(function(){
			return self.isShardAlive(shardId);
		}).then(function(alive){
			if(!alive){
				return self._unlockCommit(docId);
			}
			return Q() //jshint ignore:line
			.delay(timeout).then(function(){
				return self._unlockCommit(docId);
			});
		});
	}).fin(function(){
		return Q.fcall(function(){
			if(!commited){
				return self._unwatch();
			}
		}).fin(function(){
			delete self.docLock[docId];
		});
	}).catch(function(e){
		logger.warn(e.stack);
	});
};

/**
 * Mark the shard is alive
 */
proto.shardHeartbeat = function(shardId){
	var self = this;
	return Q.nfcall(function(cb){
		var timeout = Math.floor(self.shardHeartbeatTimeout / 1000);
		if(timeout <= 0){
			timeout = 1;
		}
		logger.info('%s %s', self._shardHeartbeatKey(shardId), timeout);
		return self.client.setex(self._shardHeartbeatKey(shardId), timeout, 1, utils.normalizecb(cb));
	})
	.then(function(){
		logger.debug('shard %s heartbeat', shardId);
	});
};

proto.isShardAlive = function(shardId){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.exists(self._shardHeartbeatKey(shardId), utils.normalizecb(cb));
	}).then(function(ret){
		return !!ret;
	});
};

// clear all locks
// Not atomic!
proto.unlockAll = function(){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.keys(self.prefix + '*', utils.normalizecb(cb));
	}).then(function(docIds){
		return Q.nfcall(function(cb){
			var multi = self.client.multi();
			docIds.forEach(function(docId){
				multi = multi.del(docId);
			});
			multi.exec(utils.normalizecb(cb));
		});
	});
};

proto._watchDoc = function(docId){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.watch(self._docKey(docId), utils.normalizecb(cb));
	});
};

proto._unwatch = function(){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.unwatch(utils.normalizecb(cb));
	});
};

proto._docKey = function(docId){
	return this.prefix + docId;
};

proto._shardHeartbeatKey = function(shardId){
	return 'shardheartbeat:' + shardId;
};

module.exports = BackendLocker;
