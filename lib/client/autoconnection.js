'use strict';

var domain = require('domain');
var Q = require('q');
var Connection = require('./connection');
var logger = require('pomelo-logger').getLogger('memorydb-client', __filename);

// Use one separate connection in one execution scope (One connection per 'api request')
var AutoConnection = function(db){
	this.db = db;
	this.connections = {};
};

var proto = AutoConnection.prototype;

proto.close = function(){
	for(var id in this.connections){
		this.connections[id].close();
	}
};

// Execute func in a new connection
// Auto commit if execute without execption
proto.execute = function(func){
	var self = this;
	var scope = domain.create();
	scope.__memorydbconn__ = this.autoId++;

	var connection = new Connection(this.db);
	this.connections[connection._id] = connection;
	scope.__memorydbconn__ = connection._id;

	logger.info('autoconnection[%s] execute start', connection._id);

	var deferred = Q.defer();
	scope.run(function(){
		Q.fcall(function(){
			return func();
		})
		.then(function(ret){
			return Q.fcall(function(){
				return connection.commit();
			})
			.then(function(){
				deferred.resolve(ret);
				logger.info('autoconnection[%s] execute done', connection._id);
			});
		}, function(err){
			deferred.reject(err);
			connection.rollback();
			logger.warn('autoconnection[%s] execute error - %s', connection._id, err.message);
		})
		.fin(function(){
			try{
				connection.close();
				delete self.connections[connection._id];
			}
			catch(e){
			}
		});
	});
	return deferred.promise;
};

// Get connection from current scope
proto._connection = function(){
	var connectionId = process.domain ? process.domain.__memorydbconn__ : null;
	if(!connectionId || !this.connections[connectionId]){
		throw new Error('You are not in any execution scope');
	}
	return this.connections[connectionId];
};

proto.collection = function(name){
	return this._connection().collection(name);
};

proto.commit = function(){
	return this._connection().commit();
};

proto.rollback = function(){
	return this._connection().rollback();
};

module.exports = AutoConnection;
