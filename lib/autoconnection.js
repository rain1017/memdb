'use strict';

var _ = require('lodash');
var domain = require('domain');
var P = require('bluebird');
var Connection = require('./connection');
var AsyncLock = require('async-lock');
var consts = require('./consts');
var uuid = require('node-uuid');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

// Max connections per shard
var DEFAULT_MAX_CONNECTION = 64;
// Idle time before close connection
var DEFAULT_CONNECTION_IDLE_TIMEOUT = 600 * 1000;
// Max pending tasks per shard
var DEFAULT_MAX_PENDING_TASK = 1024;

var DEFAULT_RECONNECT_INTERVAL = 2000;

// Use one connection per transaction
// Route request to shards
var AutoConnection = function(opts){
    opts = opts || {};

    this.db = opts.db;

    this.config = {
        maxConnection : opts.maxConnection || DEFAULT_MAX_CONNECTION,
        connectionIdleTimeout : opts.connectionIdleTimeout || DEFAULT_CONNECTION_IDLE_TIMEOUT,
        maxPendingTask : opts.maxPendingTask || DEFAULT_MAX_PENDING_TASK,
        reconnectInterval : opts.reconnectInterval || DEFAULT_RECONNECT_INTERVAL,

        // {shardId : {host : '127.0.0.1', port : 31017}}
        shards : opts.shards || {},
    };

    var shardIds = Object.keys(this.config.shards);
    if(shardIds.length === 0){
        throw new Error('please specify opts.shards');
    }

    var shards = {};
    shardIds.forEach(function(shardId){
        shards[shardId] = {
            connections : {}, // {connId : connection}
            freeConnections : {}, // {connId : true}
            connectionTimeouts : {}, // {connId : timeout}
            pendingTasks : [],
            pendingTaskStart : 0, //start index for pendingTasks
            reconnectInterval : null,
        };
    });
    this.shards = shards;

    this.openConnectionLock = new AsyncLock({Promise : P, maxPending : 10000});
    this.collections = {};
};

var proto = AutoConnection.prototype;

proto.close = function(){
    var self = this;
    // Close all connections to all shards
    return P.map(Object.keys(this.shards), function(shardId){
        var shard = self.shards[shardId];

        clearInterval(shard.reconnectInterval);
        shard.reconnectInterval = null;

        // reject all pending tasks
        var tasks = shard.pendingTasks.slice(shard.pendingTaskStart);
        shard.pendingTasks = [];
        shard.pendingTaskStart = 0;

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
    var self = this;

    return this.openConnectionLock.acquire(shardId, function(){

        var shard = self._shard(shardId);
        if(Object.keys(shard.connections).length >= self.config.maxConnection){
            return;
        }

        var conn = new Connection({
                                host : self.config.shards[shardId].host,
                                port : self.config.shards[shardId].port,
                                idleTimeout : self.config.connectionIdleTimeout
                            });

        return conn.connect()
        .then(function(connId){
            clearInterval(shard.reconnectInterval);
            shard.reconnectInterval = null;

            shard.connections[connId] = conn;

            logger.info('[shard:%s][conn:%s] open connection', shardId, connId);

            shard.freeConnections[connId] = true;

            conn.on('close', function(){
                logger.info('[shard:%s][conn:%s] connection closed', shardId, connId);
                delete shard.connections[connId];
                delete shard.freeConnections[connId];
            });

            setImmediate(function(){
                P.try(function(){
                    return self._runTask(shardId);
                })
                .catch(function(e){
                    logger.error(e.stack);
                });
            });
        }, function(e){
            if(!shard.reconnectInterval){
                shard.reconnectInterval = setInterval(function(){
                    P.try(function(){
                        return self.openConnection(shardId);
                    })
                    .catch(function(e){
                        logger.error(e.stack);
                    });
                }, self.config.reconnectInterval);
            }
            logger.error(e.stack);
        });
    });
};

proto.transaction = function(func, shardId){
    var deferred = P.defer();

    try{
        if(typeof(func) !== 'function'){
            throw new Error('Function is required');
        }
        if(!shardId){
            var shardIds = Object.keys(this.shards);
            if(shardIds.length > 1){
                throw new Error('You must specify shardId');
            }
            shardId = shardIds[0];
        }

        var shard = this._shard(shardId);

        if(shard.pendingTasks.length - shard.pendingTaskStart >= this.config.maxPendingTask){
            throw new Error('Too much pending tasks');
        }

        shard.pendingTasks.push({
            func : func,
            deferred : deferred
        });

        var self = this;
        setImmediate(function(){
            P.try(function(){
                return self._runTask(shardId);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });
    }
    catch(err){
        return deferred.reject(err);
    }
    finally{
        return deferred.promise;
    }
};

proto._runTask = function(shardId){
    var shard = this._shard(shardId);

    if(shard.pendingTasks.length - shard.pendingTaskStart === 0){
        return;
    }

    var connIds = Object.keys(shard.freeConnections);
    if(connIds.length === 0){
        return this.openConnection(shardId);
    }

    var connId = connIds[0];
    var conn = shard.connections[connId];
    delete shard.freeConnections[connId];

    var task = shard.pendingTasks[shard.pendingTaskStart];
    shard.pendingTaskStart++;
    if(shard.pendingTaskStart >= this.config.maxPendingTask){
        shard.pendingTasks.splice(0, shard.pendingTaskStart); //release memory
        shard.pendingTaskStart = 0;
    }

    var scope = domain.create();
    scope.__memdb__ = {shard: shardId, conn: connId, trans : uuid.v4()};

    var self = this;
    scope.run(function(){
        logger.info('[shard:%s][conn:%s] transaction start', shardId, connId);

        var startTick = Date.now();

        return P.try(function(){
            return task.func();
        })
        .then(function(ret){
            return conn.commit()
            .then(function(){
                logger.info('[shard:%s][conn:%s] transaction done (%sms)', shardId, connId, Date.now() - startTick);
                delete scope.__memdb__;
                task.deferred.resolve(ret);
            });
        }, function(err){
            return conn.rollback()
            .then(function(){
                logger.error('[shard:%s][conn:%s] transaction error %s', shardId, connId, err.stack);
                delete scope.__memdb__;
                task.deferred.reject(err);
            });
        })
        .catch(function(e){
            logger.error(e.stack);
            task.deferred.reject(e);
        })
        .finally(function(){
            if(shard.connections.hasOwnProperty(connId)){
                shard.freeConnections[connId] = true;
            }

            setImmediate(function(){
                P.try(function(){
                    return self._runTask(shardId);
                })
                .catch(function(e){
                    logger.error(e.stack);
                });
            });
        });
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
