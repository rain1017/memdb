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
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var clientPool = require('./clientpool');
var consts = require('./consts');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

var Connection = function(opts){
    EventEmitter.call(this);

    opts = opts || {};
    this.config = opts;
    this._client = null;
    this._connId = null;

    this._collections = {};
};

util.inherits(Connection, EventEmitter);

var proto = Connection.prototype;

proto.connect = function(){
    if(this._connId){
        throw new Error('already connected');
    }
    var key = this.config.host + ':' + this.config.port;

    return P.bind(this)
    .then(function(){
        return clientPool.getClient(this.config.host, this.config.port);
    })
    .then(function(client){
        this._client = client;
        this._client.on('close', this._close.bind(this));

        return this._client.request(null, 'connect', [consts.version]);
    })
    .then(function(ret){
        this._connId = ret.connId;

        logger.info('[conn:%s] connected on %s:%s', this._connId, this.config.host, this.config.port);
        return this._connId;
    });
};

proto.close = function(){
    if(!this._connId){
        throw new Error('not connected');
    }
    return P.bind(this)
    .then(function(){
        return this._client.request(this._connId, 'disconnect', []);
    })
    .then(function(){
        this._close();
    });
};

proto._close = function(){
    logger.info('[conn:%s] closed on %s:%s', this._connId, this.config.host, this.config.port);

    this._client = null;
    this._connId = null;
    this.emit('close');
};

proto.collection = function(name){
    var self = this;
    if(!this._collections[name]){
        var collection = {};

        consts.collMethods.forEach(function(method){
            collection[method] = function(){
                var args = [name].concat([].slice.call(arguments));
                return self[method].apply(self, args);
            };
        });

        this._collections[name] = collection;
    }
    return this._collections[name];
};

consts.connMethods.concat(consts.collMethods).forEach(function(method){
    proto[method] = function(){
        if(!this._connId){
            throw new Error('not connected');
        }
        var args = [].slice.call(arguments);
        return this._client.request(this._connId, method, args);
    };
});

module.exports = Connection;
