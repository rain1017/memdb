'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var util = require('util');
var redis = P.promisifyAll(require('redis'));

var BackendLocker = function(opts){
    opts = opts || {};

    this.shardId = opts.shardId;
    this.config = {
        host : opts.host || '127.0.0.1',
        port : opts.port || 6379,
        db : opts.db || 0,
        options : opts.options || {},
        prefix : opts.prefix || 'bl$',
        heartbeatPrefix  : opts.heartbeatPrefix || 'hb$',
        heartbeatTimeout : opts.heartbeatTimeout,
        heartbeatInterval : opts.heartbeatInterval,
    };

    this.client = null;
    this.heartbeatInterval = null;

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shardId);
};

var proto = BackendLocker.prototype;

proto.start = function(){
    return P.bind(this)
    .then(function(){
        this.client = redis.createClient(this.config.port, this.config.host, this.config.options);
        var self = this;
        this.client.on('error', function(err){
            self.logger.error(err.stack);
        });
        return this.client.selectAsync(this.config.db);
    })
    .then(function(){
        return this.isAlive();
    })
    .then(function(ret){
        if(ret){
            throw new Error('Current shard is running in some other process');
        }
    })
    .then(function(){
        if(this.config.heartbeatInterval > 0){
            return this.heartbeat();
        }
    })
    .then(function(){
        if(this.config.heartbeatInterval > 0){
            this.heartbeatInterval = setInterval(this.heartbeat.bind(this), this.config.heartbeatInterval);
        }
        this.logger.info('backendLocker started %s:%s:%s', this.config.host, this.config.port, this.config.db);
    });
};

proto.stop = function(){
    return P.bind(this)
    .then(function(){
        clearInterval(this.heartbeatInterval);
        return this.clearHeartbeat();
    })
    .then(function(){
        return this.client.quitAsync();
    })
    .then(function(){
        this.logger.info('backendLocker stoped');
    });
};

proto.tryLock = function(docId){
    this.logger.debug('tryLock %s', docId);

    var self = this;
    return this.client.setnxAsync(this._docKey(docId), this.shardId)
    .then(function(ret){
        if(ret === 1){
            self.logger.debug('locked %s', docId);
            return true;
        }
        else{
            return false;
        }
    });
};

proto.getHolderId = function(docId){
    return this.client.getAsync(this._docKey(docId));
};

proto.isHeld = function(docId){
    var self = this;
    return this.getHolderId(docId)
    .then(function(ret){
        return ret === self.shardId;
    });
};

// concurrency safe between shards
// not concurrency safe in same shard
proto.unlock = function(docId){
    this.logger.debug('unlock %s', docId);

    var self = this;
    return this.isHeld(docId)
    .then(function(held){
        if(held){
            return self.client.delAsync(self._docKey(docId));
        }
    });
};

proto.heartbeat = function(){
    var timeout = Math.floor(this.config.heartbeatTimeout / 1000);
    if(timeout <= 0){
        timeout = 1;
    }

    this.logger.debug('heartbeat');
    return this.client.setexAsync(this._heartbeatKey(this.shardId), timeout, 1);
};

proto.clearHeartbeat = function(){
    return this.client.delAsync(this._heartbeatKey(this.shardId));
};

proto.isAlive = function(shardId){
    if(!shardId){
        shardId = this.shardId;
    }
    return this.client.existsAsync(this._heartbeatKey(shardId))
    .then(function(ret){
        return !!ret;
    });
};

proto.getActiveShards = function(){
    var prefix = this.config.heartbeatPrefix;
    return this.client.keysAsync(prefix + '*')
    .then(function(keys){
        return keys.map(function(key){
            return key.slice(prefix.length);
        });
    });
};

proto._docKey = function(docId){
    return this.config.prefix + docId;
};

proto._heartbeatKey = function(shardId){
    return this.config.heartbeatPrefix + shardId;
};

module.exports = BackendLocker;
