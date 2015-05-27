'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var utils = require('./utils');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Connection = require('./connection');
var Shard = require('./shard');
var consts = require('./consts');
var AsyncLock = require('async-lock');

// Extend promise
utils.extendPromise(P);

var Database = function(opts){
    // clone since we want to modify it
    opts = utils.clone(opts) || {};

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + opts.shardId);

    this.connections = {};
    this.connectionLock = new AsyncLock({Promise : P});

    // Parse index config
    opts.collections = opts.collections || {};

    Object.keys(opts.collections).forEach(function(name){
        var collection = opts.collections[name];
        var indexes = {};
        (collection.indexes || []).forEach(function(index){
            var indexKey = JSON.stringify(index.keys.sort());
            if(indexes[indexKey]){
                throw new Error('Duplicate index keys - ' + indexKey);
            }

            delete index.keys;
            indexes[indexKey] = index;
        });
        collection.indexes = indexes;
    });

    this.logger.info('parsed opts: %j', opts);

    this.shard = new Shard(opts);
    this.config = opts;
};

util.inherits(Database, EventEmitter);

var proto = Database.prototype;

proto.start = function(){
    var self = this;
    return this.shard.start()
    .then(function(){
        self.logger.warn('database started');
    });
};

proto.stop = function(force){
    var self = this;
    return this.shard.stop(force)
    .then(function(){
        self.logger.warn('database stoped');
    });
};

proto.connect = function(){
    var connId = utils.uuid();
    var opts = {
        _id : connId,
        shard : this.shard,
        config : this.config,
        logger : this.logger
    };
    var conn = new Connection(opts);
    this.connections[connId] = conn;

    this.logger.info('[conn:%s] connection created', connId);
    return connId;
};

proto.disconnect = function(connId){
    var conn = this.getConnection(connId);
    conn.close();
    delete this.connections[connId];

    this.logger.info('[conn:%s] connection closed', connId);
};

// Execute a command
proto.execute = function(connId, method, args){
    var self = this;

    // Query in the same connection must execute in series
    // This is usually a client bug here
    if(this.connectionLock.isBusy(connId)){
        P.try(function(){
            // Must pass error in a promise in order to get the longStackTrace
            throw new Error();
        })
        .catch(function(err){
            self.logger.warn('[conn:%s] concurrent query on same connection, bug in client? %s(%j)', connId, method, args, err);
        });
    }

    // Ensure series execution in same connection
    return this.connectionLock.acquire(connId, function(){

        self.logger.debug('[conn:%s] start %s(%j)...', connId, method, args);

        var conn = self.getConnection(connId);

        return P.try(function(){
            var func = conn[method];
            if(typeof(func) !== 'function'){
                throw new Error('unsupported command - ' + method);
            }
            return func.apply(conn, args);
        })
        .then(function(ret){
            self.logger.info('[conn:%s] %s(%j) => %j', connId, method, args, ret);
            return ret;
        }, function(err){
            self.logger.error('[conn:%s] %s(%j) =>', connId, method, args, err.stack);

            // Roll back on exception
            conn.rollback();
            // Rethrow to client
            throw err;
        });
    });
};

proto.getConnection = function(id){
    var conn = this.connections[id];
    if(!conn){
        throw new Error('connection ' + id + ' not exist');
    }
    return conn;
};

module.exports = Database;
