'use strict';

var P = require('bluebird');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Client = require('./client');
var consts = require('./consts');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

var DEFAULT_IDLE_TIMEOUT = 60 * 1000;

var Connection = function(opts){
    EventEmitter.call(this);

    opts = opts || {};
    this.host = opts.host;
    this.port = opts.port;
    this.client = new Client();

    this.idleTimeoutValue = opts.hasOwnProperty('idleTimeout') ? opts.idleTimeout : DEFAULT_IDLE_TIMEOUT;
    this.idleTimeout = null;
    this.collections = {};
};

util.inherits(Connection, EventEmitter);

var proto = Connection.prototype;

proto.connect = function(){
    return P.bind(this)
    .then(function(){
        return P.bind(this)
        .then(function(){
            return this.client.connect(this.host, this.port);
        })
        .then(function(){
            return this.client.request('info', []);
        })
        .then(function(info){
            this.connId = info.connId;
        });
    })
    .then(function(){
        this.setIdleTimeout();

        var self = this;
        consts.connMethods.concat(consts.collMethods).forEach(function(method){
            self[method] = function(){
                var args = [].slice.call(arguments);
                self.setIdleTimeout();
                return self.client.request(method, args);
            };
        });

        return this.connId;
    });
};

proto.close = function(){
    this.emit('close');
    clearTimeout(this.idleTimeout);

    return this.client.disconnect();
};

proto.collection = function(name){
    var self = this;
    if(!this.collections[name]){
        var collection = {};

        consts.collMethods.forEach(function(method){
            collection[method] = function(){
                var args = [name].concat([].slice.call(arguments));
                return self[method].apply(self, args);
            };
        });

        this.collections[name] = collection;
    }
    return this.collections[name];
};

proto.setIdleTimeout = function(){
    clearTimeout(this.idleTimeout);
    if(this.idleTimeoutValue){
        this.idleTimeout = setTimeout(this.close.bind(this), this.idleTimeoutValue);
    }
};

module.exports = Connection;
