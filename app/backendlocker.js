'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var util = require('util');
var redis = P.promisifyAll(require('redis'));

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
    var db = opts.db || 0;
    var options = opts.options || {};

    this.shardId = opts.shardId;
    this.prefix = 'doc2shard$';
    this.client = redis.createClient(port, host, options);
    this.client.select(db);

    this.shardHeartbeatTimeout = opts.shardHeartbeatTimeout;

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shardId);

    this.logger.info('backend locker inited %s:%s:%s', host, port, db);
};

var proto = BackendLocker.prototype;

proto.close = function(){
    this.client.end();
    this.logger.info('backend locker close');
};

/**
 * Lock a doc, throw exception on failure
 */
proto.lock = function(docId){
    var self = this;

    return this.client.setnxAsync(this._docKey(docId), this.shardId)
    .then(function(ret){
        if(ret !== 1){
            throw new Error(docId + ' already locked by other shards');
        }
        self.logger.debug('locked %s', docId);
    });
};

/**
 * Lock a doc, return true on success, false on failure
 */
proto.tryLock = function(docId){
    this.logger.debug('tryLock %s', docId);

    var self = this;
    return this.lock(docId)
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
proto.isHeld = function(docId){
    return P.bind(this)
    .then(function(){
        return this.getHolderId(docId);
    })
    .then(function(ret){
        return ret === this.shardId;
    });
};

// Throw execption if docId not held by shardId
proto.ensureHeld = function(docId){
    return P.bind(this)
    .then(function(){
        return this.isHeld(docId);
    })
    .then(function(ret){
        if(!ret){
            throw new Error(docId + ' not held');
        }
    });
};

/**
 * unlock a doc
 * Caller must make sure it owned the doc
 */
proto.unlock = function(docId){
    var self = this;

    return this.client.delAsync(this._docKey(docId))
    .then(function(){
        self.logger.debug('unlocked %s', docId);
    });
};

/**
 * Mark the shard is alive
 */
proto.shardHeartbeat = function(){
    var timeout = Math.floor(this.shardHeartbeatTimeout / 1000);
    if(timeout <= 0){
        timeout = 1;
    }

    this.logger.debug('heartbeat');
    return this.client.setexAsync(this._shardHeartbeatKey(this.shardId), timeout, 1);
};

proto.shardStop = function(){
    return this.client.delAsync(this._shardHeartbeatKey(this.shardId));
};

proto.isShardAlive = function(shardId){
    if(!shardId){
        shardId = this.shardId;
    }

    return this.client.existsAsync(this._shardHeartbeatKey(shardId))
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
    return 'heartbeat$' + shardId;
};

module.exports = BackendLocker;
