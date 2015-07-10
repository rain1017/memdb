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
    this.host = opts.host;
    this.port = opts.port;
    this.client = null;
    this.connId = null;

    this.collections = {};
};

util.inherits(Connection, EventEmitter);

var proto = Connection.prototype;

proto.connect = function(){
    if(this.connId){
        throw new Error('already connected');
    }
    var key = this.host + ':' + this.port;

    return P.bind(this)
    .then(function(){
        return clientPool.getClient(this.host, this.port);
    })
    .then(function(client){
        this.client = client;
        this.client.on('close', this._close.bind(this));

        return this.client.request(null, 'connect');
    })
    .then(function(connId){
        this.connId = connId;

        logger.info('[conn:%s] connected on %s:%s', this.connId, this.host, this.port);
        return this.connId;
    });
};

proto.close = function(){
    if(!this.connId){
        throw new Error('not connected');
    }
    return P.bind(this)
    .then(function(){
        return this.client.request(this.connId, 'disconnect', []);
    })
    .then(function(){
        this._close();
    });
};

proto._close = function(){
    logger.info('[conn:%s] closed on %s:%s', this.connId, this.host, this.port);

    this.client = null;
    this.connId = null;
    this.emit('close');
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

consts.connMethods.concat(consts.collMethods).forEach(function(method){
    proto[method] = function(){
        if(!this.connId){
            throw new Error('not connected');
        }
        var args = [].slice.call(arguments);
        return this.client.request(this.connId, method, args);
    };
});

module.exports = Connection;
