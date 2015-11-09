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
var redis = P.promisifyAll(require('redis'));
var utils = require('./utils');

var Slave = function(opts){
    opts = opts || {};

    this.shardId = opts.shardId;

    this.config = {
        host : opts.host || '127.0.0.1',
        port : opts.port || 6379,
        db : opts.db || 0,
        options : opts.options || {},
    };

    this.client = null;
    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shardId);
};

var proto = Slave.prototype;

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
        this.logger.info('slave started %s:%s:%s', this.config.host, this.config.port, this.config.db);
    });
};

proto.stop = function(){
    return P.bind(this)
    .then(function(){
        return this.client.quitAsync();
    })
    .then(function(){
        this.logger.info('slave stoped');
    });
};

proto.set = function(key, doc){
    this.logger.debug('slave set %s', key);
    return this.client.setAsync(this._redisKey(key), JSON.stringify(doc));
};

proto.del = function(key){
    this.logger.debug('slave del %s', key);
    return this.client.delAsync(this._redisKey(key));
};

// docs - {key : doc}
proto.setMulti = function(docs){
    this.logger.debug('slave setMulti');

    var multi = this.client.multi();
    for(var key in docs){
        var doc = docs[key];
        multi = multi.set(this._redisKey(key), JSON.stringify(doc));
    }

    return multi.execAsync();
};

// returns - {key : doc}
proto.getMulti = function(keys){
    this.logger.debug('slave getMulti');

    var self = this;
    var multi = this.client.multi();
    keys.forEach(function(key){
        multi = multi.get(self._redisKey(key));
    });

    return multi.execAsync()
    .then(function(results){
        var docs = {};
        for(var i in keys){
            var key = keys[i];
            if(!!results[i]){
                docs[key] = JSON.parse(results[i]);
            }
        }
        return docs;
    });
};

proto.getAllKeys = function(){
    this.logger.debug('slave getAllKeys');

    return P.bind(this)
    .then(function(){
        return this.client.keysAsync(this._redisKey('*'));
    })
    .then(function(keys){
        var self = this;
        return keys.map(function(key){
            return self._extractKey(key);
        });
    });
};

proto.clear = function(){
    this.logger.debug('slave clear');

    return P.bind(this)
    .then(function(){
        return this.client.keysAsync(this._redisKey('*'));
    })
    .then(function(keys){
        var multi = this.client.multi();
        keys.forEach(function(key){
            multi = multi.del(key);
        });
        return multi.execAsync();
    });
};

proto._redisKey = function(key){
    return 'bk$' + this.shardId + '$' + key;
};

proto._extractKey = function(existKey){
    var words = existKey.split('$');
    return words.slice(2, words.length).join('$');
};

module.exports = Slave;
