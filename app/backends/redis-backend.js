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

var RedisBackend = function(opts){
    opts = opts || {};

    this.config = {
        host : opts.host || '127.0.0.1',
        port : opts.port || 6379,
        db : opts.db || 0,
        options : opts.option || {},
        prefix : opts.prefix || '',
    };
    this.conn = null;

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + opts.shardId);
};

var proto = RedisBackend.prototype;

proto.start = function(){
    this.conn = redis.createClient(this.config.port, this.config.host, {retry_max_delay : 10 * 1000});

    var self = this;
    this.conn.on('error', function(err){
        self.logger.error(err.stack);
    });

    this.conn.select(this.config.db);

    this.logger.debug('backend redis connected to %s:%s:%s', this.config.host, this.config.port, this.config.db);
};

proto.stop = function(){
    this.logger.debug('backend redis stop');
    return this.conn.quitAsync();
};

proto.get = function(name, id){
    this.logger.debug('backend redis get(%s, %s)', name, id);

    return P.bind(this)
    .then(function(){
        return this.conn.hmgetAsync(this.config.prefix + name, id);
    })
    .then(function(ret){
        ret = ret[0];
        return JSON.parse(ret);
    });
};

// Return an async iterator with .next(cb) signature
proto.getAll = function(name){
    throw new Error('not implemented');
};

// delete when doc is null
proto.set = function(name, id, doc){
    this.logger.debug('backend redis set(%s, %s)', name, id);

    if(!!doc){
        return this.conn.hmsetAsync(this.config.prefix + name, id, JSON.stringify(doc));
    }
    else{
        return this.conn.hdelAsync(this.config.prefix + name, id);
    }
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
    this.logger.debug('backend redis setMulti');

    var multi = this.conn.multi();

    var self = this;
    items.forEach(function(item){
        if(!!item.doc){
            multi = multi.hmset(self.config.prefix + item.name, item.id, JSON.stringify(item.doc));
        }
        else{
            multi = multi.hdel(self.config.prefix + item.name, item.id);
        }
    });
    return multi.execAsync();
};

// drop table or database
proto.drop = function(name){
    this.logger.debug('backend redis drop %s', name);

    if(!!name){
        throw new Error('not implemented');
        //this.conn.delAsync(this.config.prefix + name);
    }
    else{
        this.conn.flushdbAsync();
    }
};

proto.getCollectionNames = function(){
    throw new Error('not implemented');
};

module.exports = RedisBackend;
