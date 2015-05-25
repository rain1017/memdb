'use strict';

var P = require('bluebird');
var utils = require('./utils');
utils.extendPromise(P);

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Collection = require('./collection');
var Connection = require('./connection');
var Shard = require('./shard');
var consts = require('./consts');
var AsyncLock = require('async-lock');

var Database = function(opts){
    // clone since we want to modify it
    opts = utils.clone(opts) || {};

    this.logger = opts.logger || require('memdb-logger').getLogger('memdb', __filename, 'shard:' + opts.shardId);

    this.collections = {};
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

    opts.logger = this.logger;
    this.shard = new Shard(opts);

    this.config = opts;
};

util.inherits(Database, EventEmitter);

var proto = Database.prototype;

proto.start = function(){
    return this.shard.start();
};

proto.stop = function(force){
    return this.shard.stop(force);
};

proto.connect = function(){
    var conn = new Connection({_id : utils.uuid()});
    this.connections[conn._id] = conn;
    this.logger.info('[conn:%s] connection created', conn._id);
    return conn._id;
};

proto.disconnect = function(connId){
    var conn = this._connection(connId);
    if(conn.isDirty()){
        this.rollback(connId);
    }
    delete this.connections[connId];
    this.logger.info('[conn:%s] connection closed', connId);
};

proto.execute = function(connId, method, args){
    var self = this;

    if(this.connectionLock.isBusy(connId)){
        P.try(function(){
            throw new Error();
        })
        .catch(function(err){
            self.logger.warn('[conn:%s] concurrent query on same connection. %s(%j)', connId, method, args, err);
        });
    }

    this.logger.debug('[conn:%s] %s(%j)...', connId, method, args);

    return this.connectionLock.acquire(connId, function(){
        return P.try(function(){
            var func = self[method];
            if(typeof(func) !== 'function'){
                throw new Error('unsupported method - ' + method);
            }
            return func.apply(self, [connId].concat(args));
        })
        .then(function(ret){
            self.logger.info('[conn:%s] %s(%j) => %j', connId, method, args, ret);
            return ret;
        }, function(err){
            self.logger.error('[conn:%s] %s(%j) =>', connId, method, args, err.stack);
            self.rollback(connId);
            throw err;
        });
    });
};

consts.collMethods.forEach(function(method){
    proto[method] = function(connId, name){
        var conn = this._connection(connId);
        var collection = this._collection(name);
        var args = [].slice.call(arguments);
        args.splice(1, 1); //remove 'name' argument
        return collection[method].apply(collection, args);
    };
});

proto.commit = function(connId){
    var conn = this._connection(connId);
    return P.bind(this)
    .then(function(){
        return this.shard.commit(connId, conn.getLockedKeys());
    })
    .then(function(){
        conn.clearLockedKeys();
        this.logger.info('[conn:%s] commited', connId);
    });
};

proto.rollback = function(connId){
    var conn = this._connection(connId);
    var self = this;
    conn.getLockedKeys().forEach(function(key){
        self.shard.rollback(connId, key);
    });
    conn.clearLockedKeys();
    this.logger.info('[conn:%s] rolledback', connId);
};

proto.flushBackend = function(){
    return this.shard.flushBackend();
};

proto._collection = function(name){
    var self = this;
    if(!this.collections[name]){
        var collection = new Collection({
            name : name,
            shard : this.shard,
            db : this,
            config : this.config.collections[name] || {},
            logger : this.logger,
        });
        collection.on('lock', function(connId, id){
            var conn = self._connection(connId);
            conn.addLockedKey(name + ':' + id);
        });
        this.collections[name] = collection;
    }
    return this.collections[name];
};

proto._connection = function(id){
    var conn = this.connections[id];
    if(!conn){
        throw new Error('connection ' + id + ' not exist');
    }
    return conn;
};

module.exports = Database;
