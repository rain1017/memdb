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
var mongodb = P.promisifyAll(require('mongodb'));

var MongoBackend = function(opts){
    opts = opts || {};

    this.config = {
        url : opts.url || 'mongodb://localhost/test',
        options : {server : {socketOptions : {autoReconnect : true}, reconnectTries : 10000000, reconnectInterval : 5000}}, //always retry
    };
    this.conn = null;
    this.connected = false;
    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + opts.shardId);
};

var proto = MongoBackend.prototype;

proto.start = function(){
    var self = this;

    return P.promisify(mongodb.MongoClient.connect)(this.config.url, this.config.options)
    .then(function(ret){
        self.conn = ret;
        self.connected = true;

        self.conn.on('close', function(){
            self.connected = false;
            self.logger.error('backend mongodb disconnected');
        });

        self.conn.on('reconnect', function(){
            self.connected = true;
            self.logger.warn('backend mongodb reconnected');
        });

        self.conn.on('error', function(err){
            self.logger.error(err.stack);
        });

        self.logger.info('backend mongodb connected to %s', self.config.url);
    });
};

proto.stop = function(){
    var self = this;

    this.conn.removeAllListeners('close');

    return this.conn.closeAsync()
    .then(function(){
        self.logger.info('backend mongodb closed');
    });
};

proto.get = function(name, id){
    this.ensureConnected();
    this.logger.debug('backend mongodb get(%s, %s)', name, id);

    return this.conn.collection(name).findOneAsync({_id : id});
};

// Return an async iterator with .next(cb) signature
proto.getAll = function(name){
    this.ensureConnected();
    this.logger.debug('backend mongodb getAll(%s)', name);

    return this.conn.collection(name).findAsync();
};

proto.set = function(name, id, doc){
    this.ensureConnected();
    this.logger.debug('backend mongodb set(%s, %s)', name, id);

    if(!!doc){
        doc._id = id;
        return this.conn.collection(name).updateAsync({_id : id}, doc, {upsert : true});
    }
    else{
        return this.conn.collection(name).removeAsync({_id : id});
    }
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
    this.ensureConnected();
    this.logger.debug('backend mongodb setMulti');

    var self = this;
    return P.mapLimit(items, function(item){
        return self.set(item.name, item.id, item.doc);
    });
};

// drop table or database
proto.drop = function(name){
    this.ensureConnected();
    this.logger.debug('backend mongodb drop %s', name);

    if(!!name){
        return this.conn.collection(name).dropAsync()
        .catch(function(e){
            // Ignore ns not found error
            if(e.message.indexOf('ns not found') === -1){
                throw e;
            }
        });
    }
    else{
        return this.conn.dropDatabaseAsync();
    }
};

proto.getCollectionNames = function(){
    return this.conn.collectionsAsync().then(function(collections){
        return collections.map(function(collection){
            return collection.s.name;
        });
    });
};

proto.ensureConnected = function(){
    if(!this.connected){
        throw new Error('backend mongodb not connected');
    }
};

module.exports = MongoBackend;
