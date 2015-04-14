'use strict';

var Q = require('q');
var util = require('util');
var utils = require('./utils');
var redis = require('redis');
var logger = require('pomelo-logger').getLogger('memdb', __filename);

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
	var options = opts.options || {};

	this.prefix = 'doc2shard:';
	this.client = redis.createClient(port, host, options);

	this.shardHeartbeatTimeout = opts.shardHeartbeatTimeout || DEFAULT_SHARD_HEARTBEAT_TIMEOUT;
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

/**
 * Get lock holder shardIds for each docId
 */
proto.getHolderIdMulti = function(docIds){
	var self = this;
	var multi = self.client.multi();
	docIds.forEach(function(docId){
		multi = multi.get(self._docKey(docId));
	});
	return Q.nfcall(function(cb){
		multi.exec(utils.normalizecb(cb));
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
 * Caller must make sure it owned the doc
 */
proto.unlock = function(docId){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.del(self._docKey(docId), utils.normalizecb(cb));
	}).then(function(){
		logger.debug('unlocked %s', docId);
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
		return self.client.setex(self._shardHeartbeatKey(shardId), timeout, 1, utils.normalizecb(cb));
	})
	.then(function(){
		logger.debug('shard %s heartbeat', shardId);
	});
};

proto.shardStop = function(shardId){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.del(self._shardHeartbeatKey(shardId), utils.normalizecb(cb));
	})
	.then(function(){
		logger.debug('shard %s stop', shardId);
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

proto._docKey = function(docId){
	return this.prefix + docId;
};

proto._shardHeartbeatKey = function(shardId){
	return 'shardheartbeat:' + shardId;
};

module.exports = BackendLocker;
