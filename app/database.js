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
var utils = require('./utils');
var util = require('util');
var os = require('os');
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

    this.connections = utils.forceHashMap();
    this.connectionLock = new AsyncLock({Promise : P});

    this.dbWrappers = utils.forceHashMap(); //{connId : dbWrapper}

    this.opsCounter = utils.rateCounter();
    this.tpsCounter = utils.rateCounter();

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

    this.timeCounter = utils.timeCounter();
};

util.inherits(Database, EventEmitter);

var proto = Database.prototype;

proto.start = function(){
    var self = this;
    return this.shard.start()
    .then(function(){
        self.logger.info('database started');
    });
};

proto.stop = function(force){
    var self = this;

    this.opsCounter.stop();
    this.tpsCounter.stop();

    return P.try(function(){
        // Make sure no new request come anymore

        // Wait for all operations finish
        return utils.waitUntil(function(){
            return !self.connectionLock.isBusy();
        });
    })
    .then(function(){
        self.logger.debug('all requests finished');
        return self.shard.stop(force);
    })
    .then(function(){
        self.logger.info('database stoped');
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
    return {
        connId : connId,
    };
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

    if(method === 'info'){
        return {
            connId : connId,
            ver : consts.version,
            uptime : process.uptime(),
            mem : process.memoryUsage(),
            // rate for last 1, 5, 15 minutes
            ops : [this.opsCounter.rate(60), this.opsCounter.rate(300), this.opsCounter.rate(900)],
            tps : [this.tpsCounter.rate(60), this.tpsCounter.rate(300), this.tpsCounter.rate(900)],
            lps : [this.shard.loadCounter.rate(60), this.shard.loadCounter.rate(300), this.shard.loadCounter.rate(900)],
            ups : [this.shard.unloadCounter.rate(60), this.shard.unloadCounter.rate(300), this.shard.unloadCounter.rate(900)],
            pps : [this.shard.persistentCounter.rate(60), this.shard.persistentCounter.rate(300), this.shard.persistentCounter.rate(900)],
            counter : this.timeCounter.getCounts(),
        };
    }
    else if(method === 'resetCounter'){
        this.opsCounter.reset();
        this.tpsCounter.reset();
        this.shard.loadCounter.reset();
        this.shard.unloadCounter.reset();
        this.shard.persistentCounter.reset();

        this.timeCounter.reset();
        return;
    }
    else if(method === 'eval'){
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
        var err = new Error(util.format('[conn:%s] concurrent query on same connection. %s(%j)', connId, method, args));
        this.logger.error(err);
        throw err;
    }

    // Ensure series execution in same connection
    return this.connectionLock.acquire(connId, function(cb){
        self.logger.debug('[conn:%s] start %s(%j)...', connId, method, args);
        if(method === 'commit' || method === 'rollback'){
            self.tpsCounter.inc();
        }
        else{
            self.opsCounter.inc();
        }

        var hrtimer = utils.hrtimer(true);
        var conn = null;

        return P.try(function(){
            conn = self.getConnection(connId);

            var func = conn[method];
            if(typeof(func) !== 'function'){
                throw new Error('unsupported command - ' + method);
            }
            return func.apply(conn, args);
        })
        .then(function(ret){
            var timespan = hrtimer.stop();
            var level = timespan < self.config.slowQuery ? 'info' : 'warn'; // warn slow query
            self.logger[level]('[conn:%s] %s(%j) => %j (%sms)', connId, method, args, ret, timespan);

            var category = method;
            if(consts.collMethods.indexOf(method) !== -1){
                category += ':' + args[0];
            }
            self.timeCounter.add(category, timespan);

            return ret;
        }, function(err){
            var timespan = hrtimer.stop();
            self.logger.error('[conn:%s] %s(%j) => %s (%sms)', connId, method, args, err.stack ? err.stack : err, timespan);

            if(conn){
                conn.rollback();
            }

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
