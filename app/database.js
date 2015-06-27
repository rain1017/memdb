'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var utils = require('./utils');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Connection = require('./connection');
var Shard = require('./shard');
var consts = require('./consts');
var vm = require('vm');
var AsyncLock = require('async-lock');
var _ = require('lodash');

var DEFAULT_SLOWQUERY = 2000;

// Extend promise
utils.extendPromise(P);

var Database = function(opts){
    // clone since we want to modify it
    opts = utils.clone(opts) || {};

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + opts.shardId);

    this.connections = {};
    this.connectionLock = new AsyncLock({Promise : P});

    this.dbWrappers = {}; //{connId : dbWrapper}

    opts.slowQuery = opts.slowQuery || DEFAULT_SLOWQUERY;

    // Parse index config
    opts.collections = opts.collections || {};

    Object.keys(opts.collections).forEach(function(name){
        var collection = opts.collections[name];
        var indexes = {};
        (collection.indexes || []).forEach(function(index){
            if(!Array.isArray(index.keys)){
                index.keys = [index.keys];
            }
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
        if(typeof(process.send) === 'function'){
            process.send('start');
        }
        self.logger.warn('database started');
    });
};

proto.stop = function(force){
    var self = this;

    return P.try(function(){
        // Make sure no new request come anymore

        // Wait for all operations finish
        return utils.waitUntil(function(){
            return !self.connectionLock.isBusy();
        });
    })
    .then(function(){
        return self.shard.stop(force);
    })
    .then(function(){
        if(typeof(process.send) === 'function'){
            process.send('stop');
        }
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

    var self = this;
    var dbWrapper = {};
    consts.collMethods.concat(consts.connMethods).forEach(function(method){
        dbWrapper[method] = function(){
            return self.execute(connId, method, [].slice.call(arguments));
        };
    });
    this.dbWrappers[connId] = dbWrapper;

    this.logger.info('[conn:%s] connection created', connId);
    return connId;
};

proto.disconnect = function(connId){
    var self = this;
    return P.try(function(){
        var conn = self.getConnection(connId);
        return self.execute(connId, 'close', [], {ignoreConcurrent : true});
    })
    .then(function(){
        delete self.connections[connId];
        delete self.dbWrappers[connId];
        self.logger.info('[conn:%s] connection closed', connId);
    });
};

// Execute a command
proto.execute = function(connId, method, args, opts){
    opts = opts || {};
    var self = this;

    if(method[0] === '$'){ // Internal method (allow concurrent call)
        var conn = this.getConnection(connId);
        return conn[method].apply(conn, args);
    }

    if(method === 'eval'){
        var script = args[0] || '';
        var sandbox = args[1] || {};
        sandbox.require = require;
        sandbox.P = P;
        sandbox._ = _;
        sandbox.db = this.dbWrappers[connId];

        var context = vm.createContext(sandbox);

        return vm.runInContext(script, context);
    }

    // Query in the same connection must execute in series
    // This is usually a client bug here
    if(this.connectionLock.isBusy(connId) && !opts.ignoreConcurrent){
        P.try(function(){
            // Must pass error in a promise in order to get the longStackTrace
            throw new Error();
        })
        .catch(function(err){
            self.logger.warn('[conn:%s] concurrent query on same connection, bug in client? %s(%j)', connId, method, args, err);
        });
    }

    // Ensure series execution in same connection
    return this.connectionLock.acquire(connId, function(cb){

        self.logger.debug('[conn:%s] start %s(%j)...', connId, method, args);

        var conn = self.getConnection(connId);
        var startTick = Date.now();

        return P.try(function(){
            var func = conn[method];
            if(typeof(func) !== 'function'){
                throw new Error('unsupported command - ' + method);
            }
            return func.apply(conn, args);
        })
        .then(function(ret){
            var timespan = Date.now() - startTick;
            var level = timespan < self.config.slowQuery ? 'info' : 'warn'; // warn slow query
            self.logger[level]('[conn:%s] %s(%j) => %j (%sms)', connId, method, args, ret, timespan);
            return ret;
        }, function(err){
            var timespan = Date.now() - startTick;
            self.logger.error('[conn:%s] %s(%j) => %s (%sms)', connId, method, args, err.stack ? err.stack : err, timespan);

            conn.rollback();

            // Rethrow to client
            throw err;
        })
        .nodeify(cb);
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
