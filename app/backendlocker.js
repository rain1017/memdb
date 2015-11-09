// Copyright 2015 rain1017.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

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
        this.client = redis.createClient(this.config.port, this.config.host, {retry_max_delay : 10 * 1000, enable_offline_queue : true});
        var self = this;
        this.client.on('error', function(err){
            self.logger.error(err.stack);
        });
        return this.client.selectAsync(this.config.db);
    })
    .then(function(){
        if(this.shardId){
            return this.isAlive()
            .then(function(ret){
                if(ret){
                    throw new Error('Current shard is running in some other process');
                }
            });
        }
    })
    .then(function(){
        if(this.shardId && this.config.heartbeatInterval > 0){
            this.heartbeatInterval = setInterval(this.heartbeat.bind(this), this.config.heartbeatInterval);
            return this.heartbeat();
        }
    })
    .then(function(){
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

proto.tryLock = function(docId, shardId){
    this.logger.debug('tryLock %s', docId);

    var self = this;
    return this.client.setnxAsync(this._docKey(docId), shardId || this.shardId)
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

proto.isHeld = function(docId, shardId){
    var self = this;
    return this.getHolderId(docId)
    .then(function(ret){
        return ret === (shardId || self.shardId);
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

    var self = this;
    return this.client.setexAsync(this._heartbeatKey(this.shardId), timeout, 1)
    .then(function(){
        self.logger.debug('heartbeat');
    })
    .catch(function(err){
        self.logger.error(err.stack);
    });
};

proto.clearHeartbeat = function(){
    return this.client.delAsync(this._heartbeatKey(this.shardId));
};

proto.isAlive = function(shardId){
    return this.client.existsAsync(this._heartbeatKey(shardId || this.shardId))
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
