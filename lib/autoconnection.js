'use strict';

var _ = require('lodash');
var domain = require('domain');
var P = require('bluebird');
var Connection = require('./connection');
var consts = require('../app/consts');
var utils = require('../app/utils');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

// Max connections per shard
var DEFAULT_MAX_CONNECTION = 32;
// Idle time before close connection
var DEFAULT_CONNECTION_IDLE_TIMEOUT = 60 * 1000;
// Max pending tasks
var DEFAULT_MAX_PENDING_TASK = 128;

// Use one connection per transaction
// Route request to shards
var AutoConnection = function(opts){
    opts = opts || {};

    this.db = opts.db;

    this.config = {
        maxConnection : opts.maxConnection || DEFAULT_MAX_CONNECTION,
        connectionIdleTimeout : opts.connectionIdleTimeout || DEFAULT_CONNECTION_IDLE_TIMEOUT,
        maxPendingTask : opts.maxPendingTask || DEFAULT_MAX_PENDING_TASK,

        // {shardId : {host : '127.0.0.1', port : 31017}}
        shards : opts.shards || {},
    };

    var shardIds = Object.keys(this.config.shards);
    if(shardIds.length > 0){
        if(this.db){
            throw new Error('You can not specify both in-process and stand-alone shards');
        }
        this.inprocess = false;
    }
    else{
        if(!this.db){
            throw new Error('Neither in-process or any standalone shard is provided');
        }
        // in-process connection use shardId = ''
        shardIds = [''];
        this.inprocess = true;
    }

    var shards = {};
    shardIds.forEach(function(shardId){
        shards[shardId] = {
            connections : {}, // {connId : connection}
            freeConnections : {}, // {connId : true}
            connectionTimeouts : {}, // {connId : timeout}
            pendingTasks : [],
        };
    });
    this.shards = shards;

    this.collections = {};
};

var proto = AutoConnection.prototype;

proto.close = function(){
    var self = this;
    // Close all connections to all shards
    return P.map(Object.keys(this.shards), function(shardId){
        var shard = self.shards[shardId];

        // reject all pending tasks
        var tasks = shard.pendingTasks;
        shard.pendingTasks = [];
        return P.map(tasks, function(task){
            task.deferred.reject(new Error('connection closed'));
        })
        .then(function(){
            // close all connections
            var connections = shard.connections;
            return P.map(Object.keys(connections), function(connId){
                var conn = connections[connId];
                if(conn){
                    return conn.close();
                }
            });
        });
    });
};

proto.openConnection = function(shardId){
    var shard = this._shard(shardId);
    if(Object.keys(shard.connections).length >= this.config.maxConnection){
        return;
    }

    var conn = null;
    if(shardId === ''){ // in-process shard
        conn = new Connection({db : this.db});
    }
    else{
        conn = new Connection({
                            host : this.config.shards[shardId].host,
                            port : this.config.shards[shardId].port,
                            idleTimeout : this.config.connectionIdleTimeout
                        });
    }

    var self = this;

    return conn.connect()
    .then(function(connId){
        shard.connections[connId] = conn;

        logger.info('[shard:%s][conn:%s] open connection', shardId, connId);

        shard.freeConnections[connId] = true;

        conn.on('close', function(){
            logger.info('[shard:%s][conn:%s] connection closed', shardId, connId);
            delete shard.connections[connId];
            delete shard.freeConnections[connId];
        });

        setImmediate(self._runTask.bind(self, shardId));
    });
};

proto.transaction = function(func, shardId){
    if(typeof(func) !== 'function'){
        throw new Error('You must specify a function');
    }
    if(!this.inprocess){
        if(!shardId){
            throw new Error('You must specify shardId in stand-alone mode');
        }
    }
    else{
        if(shardId){
            throw new Error('Can not specify shardId in in-process mode');
        }
        shardId = '';
    }

    var shard = this._shard(shardId);

    if(shard.pendingTasks.length >= this.config.maxPendingTask){
        throw new Error('Too much pending tasks');
    }

    var deferred = P.defer();
    shard.pendingTasks.push({
        func : func,
        deferred : deferred
    });

    setImmediate(this._runTask.bind(this, shardId));

    return deferred.promise;
};

proto._runTask = function(shardId){
    var self = this;
    P.try(function(){

        var shard = self._shard(shardId);

        if(shard.pendingTasks.length === 0){
            return;
        }

        var connIds = Object.keys(shard.freeConnections);
        if(connIds.length === 0){
            return self.openConnection(shardId);
        }
        var connId = connIds[0];
        var conn = shard.connections[connId];
        delete shard.freeConnections[connId];

        var task = shard.pendingTasks.splice(0, 1)[0];

        var scope = domain.create();
        scope.__memdb__ = {shard: shardId, conn: connId, trans : utils.uuid()};

        scope.run(function(){
            logger.info('[shard:%s][conn:%s] transaction start', shardId, connId);

            var startTick = Date.now();

            var promise = P.try(function(){
                return task.func();
            });

            return promise.then(function(ret){
                return P.try(function(){
                    return conn.commit();
                })
                .then(function(){
                    logger.info('[shard:%s][conn:%s] transaction done (%sms)', shardId, connId, Date.now() - startTick);
                    delete scope.__memdb__;
                    task.deferred.resolve(ret);
                });
            }, function(err){
                return P.try(function(){
                    return conn.rollback();
                })
                .then(function(){
                    logger.error('[shard:%s][conn:%s] transaction error %s', shardId, connId, err.stack);
                    delete scope.__memdb__;
                    task.deferred.reject(err);
                });
            })
            .then(function(){
                if(!!shard.connections[connId]){
                    shard.freeConnections[connId] = true;
                }

                setImmediate(self._runTask.bind(self, shardId));
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

    })
    .catch(function(e){
        logger.error(e.stack);
    });
};

// Get connection from current scope
proto._connection = function(){
    var info = process.domain && process.domain.__memdb__;
    if(!info){
        throw new Error('You are not in any transaction scope');
    }
    var shard = this._shard(info.shard);
    var conn = shard.connections[info.conn];
    if(!conn){
        throw new Error('connection ' + info.conn + ' not exist');
    }
    return conn;
};

proto._shard = function(shardId){
    var shard = this.shards[shardId];
    if(!shard){
        throw new Error('shard ' + shardId + ' not exist');
    }
    return shard;
};

proto.collection = function(name){
    var self = this;
    if(!this.collections[name]){
        var collection = {};

        consts.collMethods.forEach(function(method){
            collection[method] = function(){
                var conn = self._connection();
                var args = [name].concat([].slice.call(arguments));
                return conn[method].apply(conn, args);
            };
        });

        this.collections[name] = collection;
    }
    return this.collections[name];
};

consts.connMethods.forEach(function(method){
    if(method === 'commit' || method === 'rollback'){
        return;
    }

    proto[method] = function(){
        var conn = this._connection();
        return conn[method].apply(conn, arguments);
    };
});

module.exports = AutoConnection;
