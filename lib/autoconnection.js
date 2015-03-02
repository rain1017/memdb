'use strict';

var domain = require('domain');
var Q = require('q');
var Connection = require('./connection');
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

// Use one separate connection in one execution scope (One connection per 'api request')
var AutoConnection = function(opts){
	this.shard = opts.shard;
	this.connections = {};
	this.autoId = 1;
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

	var connection = new Connection({_id : scope.__memorydbconn__, shard : this.shard});
	this.connections[connection._id] = connection;

	logger.info('autoconnection[%s] execute start', connection._id);

	var deferred = Q.defer();
	scope.run(function(){
		Q.fcall(function(){
			return func();
		}).then(function(ret){
			connection.commit();
			deferred.resolve(ret);
			logger.info('autoconnection[%s] execute done', connection._id);
		}, function(err){
			deferred.reject(err);
			connection.rollback();
			logger.warn('autoconnection[%s] execute error - %s', connection._id, err.message);
		}).fin(function(){
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
proto._getConnection = function(){
	var connectionId = process.domain.__memorydbconn__;
	if(!connectionId || !this.connections[connectionId]){
		throw new Error('You are not in any execution scope');
	}
	return this.connections[connectionId];
};

proto.collection = function(name){
	return this._getConnection().collection(name);
};

proto.commit = function(){
	return this._getConnection().commit();
};

proto.rollback = function(){
	return this._getConnection().rollback();
};

module.exports = AutoConnection;
