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

var _ = require('lodash');
var domain = require('domain');
var P = require('bluebird');
var Connection = require('./connection');
var AsyncLock = require('async-lock');
var consts = require('./consts');
var uuid = require('node-uuid');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

// Max connections per shard (max concurrent transactions per shard)
var DEFAULT_MAX_CONNECTION = 32;
// Idle time before close connection
var DEFAULT_CONNECTION_IDLE_TIMEOUT = 600 * 1000;
// Max pending tasks per shard
var DEFAULT_MAX_PENDING_TASK = 2048;

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

        // allow concurrent request inside one connection, internal use only
        concurrentInConnection : opts.concurrentInConnection || false,

        // {shardId : {host : '127.0.0.1', port : 31017}}
        shards : opts.shards || {},
    };

    if(this.config.concurrentInConnection){
        this.config.maxConnection = 1;
    }

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
        shard.pendingTasks.forEach(function(task){
            task.deferred.reject(new Error('connection closed'));
        });
        shard.pendingTasks = [];

        // close all connections
        var connections = shard.connections;
        return P.map(Object.keys(connections), function(connId){
            var conn = connections[connId];
            if(conn){
                return conn.close();
            }
        });
    })
    .then(function(){
        logger.info('autoConnection closed');
    })
    .catch(function(err){
        logger.error(err.stack);
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
                return self._runTask(shardId);
            });
        }, function(e){
            if(!shard.reconnectInterval){
                shard.reconnectInterval = setInterval(function(){
                    return self.openConnection(shardId);
                }, self.config.reconnectInterval);
            }
            logger.error(e.stack);

            if(Object.keys(shard.connections).length === 0){
                logger.error('No connection available for shard %s', shardId);

                // no available connection, reject all pending tasks
                shard.pendingTasks.forEach(function(task){
                    task.deferred.reject(e);
                });
                shard.pendingTasks = [];
            }
        });
    })
    .catch(function(e){
        logger.error(e.stack);
    });
};

proto._task = function(method, args, shardId){
    var deferred = P.defer();
    try{
        if(!shardId){
            var shardIds = Object.keys(this.shards);
            if(shardIds.length > 1){
                throw new Error('You must specify shardId');
            }
            shardId = shardIds[0];
        }

        var shard = this._shard(shardId);

        if(shard.pendingTasks.length >= this.config.maxPendingTask){
            throw new Error('Too much pending tasks');
        }

        shard.pendingTasks.push({
            method : method,
            args : args,
            deferred : deferred
        });

        var self = this;
        setImmediate(function(){
            return self._runTask(shardId);
        });
    }
    catch(err){
        deferred.reject(err);
    }
    finally{
        return deferred.promise;
    }
};

proto._runTask = function(shardId){
    var self = this;
    var shard = this._shard(shardId);

    if(shard.pendingTasks.length === 0){
        return;
    }

    var connIds = Object.keys(shard.freeConnections);
    if(connIds.length === 0){
        return this.openConnection(shardId);
    }

    var connId = connIds[0];
    var conn = shard.connections[connId];
    if(!this.config.concurrentInConnection){
        delete shard.freeConnections[connId];
    }

    var task = shard.pendingTasks.shift();

    if(this.config.concurrentInConnection){
        // start run next before current task finish
        setImmediate(function(){
            self._runTask(shardId);
        });
    }

    return P.try(function(){
        if(task.method === '__t'){
            var func = task.args[0];
            return self._runTransaction(func, conn, shardId);
        }
        else{
            var method = conn[task.method];
            if(typeof(method) !== 'function'){
                throw new Error('invalid method - ' + task.method);
            }
            return method.apply(conn, task.args);
        }
    })
    .then(function(ret){
        task.deferred.resolve(ret);
    }, function(err){
        task.deferred.reject(err);
    })
    .finally(function(){
        if(shard.connections.hasOwnProperty(connId)){
            shard.freeConnections[connId] = true;
        }

        setImmediate(function(){
            self._runTask(shardId);
        });
    })
    .catch(function(e){
        logger.error(e.stack);
    });
};

proto._runTransaction = function(func, conn, shardId){
    if(typeof(func) !== 'function'){
        throw new Error('Function is required');
    }

    var deferred = P.defer();

    var scope = domain.create();
    scope.__memdb__ = {shard: shardId, conn: conn._connId, trans : uuid.v4()};

    var self = this;
    scope.run(function(){
        logger.info('[shard:%s][conn:%s] transaction start', shardId, conn._connId);

        var startTick = Date.now();

        return P.try(function(){
            return func();
        })
        .then(function(ret){
            return conn.commit()
            .then(function(){
                logger.info('[shard:%s][conn:%s] transaction done (%sms)', shardId, conn._connId, Date.now() - startTick);
                delete scope.__memdb__;
                deferred.resolve(ret);
            });
        }, function(err){
            return conn.rollback()
            .then(function(){
                logger.error('[shard:%s][conn:%s] transaction error %s', shardId, conn._connId, err.stack);
                delete scope.__memdb__;
                deferred.reject(err);
            });
        })
        .catch(function(e){
            logger.error(e.stack);
            deferred.reject(e);
        });
    });

    return deferred.promise;
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
        // Method must be called inside transaction
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
    // Methods not bind to transaction
    proto[method] = function(){
        var shardId = arguments[0];
        var args = [].slice.call(arguments, 1);
        return this._task(method, args, shardId);
    };
});

proto.transaction = function(func, shardId){
    return this._task('__t', [func], shardId);
};

module.exports = AutoConnection;
