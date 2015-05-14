'use strict';

var _ = require('lodash');
var domain = require('domain');
var P = require('bluebird');
var Connection = require('./connection');
var consts = require('../app/consts');
var utils = require('../app/utils');
var logger = require('pomelo-logger').getLogger('memdb-client', __filename);

var DEFAULT_MAX_CONNECTION = 32;
var DEFAULT_CONNECTION_IDLE_TIMEOUT = 60 * 1000;
var DEFAULT_MAX_PENDING_TASK = 128;

// Use one separate connection in one execution scope (One connection per 'request')
var AutoConnection = function(opts){
	opts = opts || {};

	this.db = opts.db;

	this.config = {};
	this.config.maxConnection = opts.maxConnection || DEFAULT_MAX_CONNECTION;
	this.config.connectionIdleTimeout = opts.connectionIdleTimeout || DEFAULT_CONNECTION_IDLE_TIMEOUT;
	this.config.maxPendingTask = opts.maxPendingTask || DEFAULT_MAX_PENDING_TASK;
	this.config.host = opts.host;
	this.config.port = opts.port;

	this.connections = {}; // {connId : connection}
	this.freeConnections = {}; // {connId : true}
	this.connectionTimeouts = {}; // {connId : timeout}

	this.pendingTasks = [];

    this.collections = {};
};

var proto = AutoConnection.prototype;

proto.close = function(){
	return P.bind(this)
	.then(function(){
		return Object.keys(this.connections);
	})
	.map(function(connId){
		var conn = this.connections[connId];
		if(conn){
			return conn.close();
		}
	});
};

proto.openConnection = function(){
	if(Object.keys(this.connections).length >= this.config.maxConnection){
		return;
	}

	var conn = new Connection({
							db : this.db,
							host : this.config.host,
							port : this.config.port,
							idleTimeout : this.config.connectionIdleTimeout
						});


	return P.bind(this)
	.then(function(){
		return conn.connect();
	})
	.then(function(connId){
		this.connections[connId] = conn;
		logger.info('open connection %s', connId);

		this.freeConnections[connId] = true;

		var self = this;
		conn.on('close', function(){
			logger.info('connection %s closed', connId);
			delete self.connections[connId];
			delete self.freeConnections[connId];
		});

		setImmediate(this._runTask.bind(this));
	})
	.catch(function(e){
		logger.error(e.stack);
	});
};

// Execute func in a new connection
// Auto commit if execute without execption
proto.transaction = function(func){
    if(typeof(func) !== 'function'){
        throw new Error('You must specify a function');
    }

	if(this.pendingTasks.length >= this.config.maxPendingTask){
		throw new Error('Too much pending tasks');
	}

	var deferred = P.defer();
	this.pendingTasks.push({
		func : func,
		deferred : deferred
	});

	setImmediate(this._runTask.bind(this));

	return deferred.promise;
};

proto._runTask = function(){
	if(this.pendingTasks.length === 0){
		return;
	}
	var connIds = Object.keys(this.freeConnections);
	if(connIds.length === 0){
	 	return this.openConnection();
	}

	var connId = connIds[0];
	var conn = this.connections[connId];
	delete this.freeConnections[connId];

	var task = this.pendingTasks.splice(0, 1)[0];

	var scope = domain.create();
	scope.__memdb__ = {conn: connId, trans : utils.uuid()};

	var self = this;
	scope.run(function(){
		logger.info('task start on connection %s', connId);

		var startTick = Date.now();

        var promise = null;
        if(task.func.length === 1){ //Callback style
            var defered = P.defer();
            task.func(function(err, ret){
                if(err){
                    defered.reject(err);
                }
                else{
                    defered.resolve(ret);
                }
            });
            promise = defered.promise;
        }
        else{
            promise = P.try(function(){
                return task.func();
            });
        }

		return promise.then(function(ret){
			return P.try(function(){
				return conn.commit();
			})
			.then(function(){
				logger.info('task done on connection %s (%sms)', connId, Date.now() - startTick);
				delete scope.__memdb__;
				task.deferred.resolve(ret);
			});
		}, function(err){
			return P.try(function(){
				return conn.rollback();
			})
			.then(function(){
				logger.error('task error on connection %s', connId, err);
				delete scope.__memdb__;
				task.deferred.reject(err);
			});
		})
		.then(function(){
			if(!!self.connections[connId]){
				self.freeConnections[connId] = true;
			}

			setImmediate(self._runTask.bind(self));
		})
		.catch(function(e){
			logger.error(e.stack);
		});
	});
};

// Get connection from current scope
proto._connection = function(){
	var connectionId = process.domain && process.domain.__memdb__ && process.domain.__memdb__.conn;
	if(!connectionId || !this.connections[connectionId]){
		throw new Error('You are not in any transaction scope');
	}
	return this.connections[connectionId];
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
