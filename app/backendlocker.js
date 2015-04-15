'use strict';

var P = require('bluebird');
var util = require('util');
var redis = P.promisifyAll(require('redis'));
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
	return P.bind(this)
	.then(function(){
		return this.client.setnxAsync(this._docKey(docId), shardId);
	})
	.then(function(ret){
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
	return P.bind(this)
	.then(function(){
		return this.lock(docId, shardId);
	})
	.then(function(){
		return true;
	}, function(err){
		return false;
	});
};

/**
 * Get lock holder shardId
 */
proto.getHolderId = function(docId){
	return this.client.getAsync(this._docKey(docId));
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
	return multi.execAsync();
};

// Whether docId is held by shardId
proto.isHeldBy = function(docId, shardId){
	return P.bind(this)
	.then(function(){
		return this.getHolderId(docId);
	})
	.then(function(ret){
		return ret === shardId;
	});
};

// Throw execption if docId not held by shardId
proto.ensureHeldBy = function(docId, shardId){
	return P.bind(this)
	.then(function(){
		return this.isHeldBy(docId, shardId);
	})
	.then(function(ret){
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
	return P.bind(this)
	.then(function(){
		return this.client.delAsync(this._docKey(docId));
	})
	.then(function(){
		logger.debug('unlocked %s', docId);
	});
};

/**
 * Mark the shard is alive
 */
proto.shardHeartbeat = function(shardId){
	return P.bind(this)
	.then(function(){
		var timeout = Math.floor(this.shardHeartbeatTimeout / 1000);
		if(timeout <= 0){
			timeout = 1;
		}
		return this.client.setexAsync(this._shardHeartbeatKey(shardId), timeout, 1);
	})
	.then(function(){
		logger.debug('shard %s heartbeat', shardId);
	});
};

proto.shardStop = function(shardId){
	return P.bind(this)
	.then(function(){
		return this.client.delAsync(this._shardHeartbeatKey(shardId));
	})
	.then(function(){
		logger.debug('shard %s stop', shardId);
	});
};

proto.isShardAlive = function(shardId){
	return P.bind(this)
	.then(function(){
		return this.client.existsAsync(this._shardHeartbeatKey(shardId));
	})
	.then(function(ret){
		return !!ret;
	});
};

// clear all locks
// Not atomic!
proto.unlockAll = function(){
	return P.bind(this)
	.then(function(){
		return this.client.keysAsync(this.prefix + '*');
	})
	.then(function(docIds){
		var multi = this.client.multi();
		docIds.forEach(function(docId){
			multi = multi.del(docId);
		});
		return multi.execAsync();
	});
};

proto._docKey = function(docId){
	return this.prefix + docId;
};

proto._shardHeartbeatKey = function(shardId){
	return 'shardheartbeat:' + shardId;
};

module.exports = BackendLocker;
