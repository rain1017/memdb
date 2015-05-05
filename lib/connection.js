'use strict';

var P = require('bluebird');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Client = require('./client');
var logger = require('pomelo-logger').getLogger('memdb-client', __filename);

var _connMethods = ['commit', 'rollback', 'persistentAll'];
var _collMethods = ['find', 'findOne', 'findById', 'findLocked', 'findOneLocked', 'findByIdLocked',
					'insert', 'update', 'remove', 'lock', 'findCached'];

var DEFAULT_IDLE_TIMEOUT = 0; //never

/**
 * opts.db - in-process db object
 * opts.host, opts.port - socket.io db server host and port
 */
var Connection = function(opts){
	EventEmitter.call(this);

	opts = opts || {};

	this.db = opts.db;
	if(!!this.db){
		this.connId = null;
	}
	else{
		this.host = opts.host;
		this.port = opts.port;
		this.client = new Client();
	}

	this.idleTimeoutValue = opts.idleTimeout || DEFAULT_IDLE_TIMEOUT;
	this.idleTimeout = null;
	this.collections = {};
};

util.inherits(Connection, EventEmitter);

var proto = Connection.prototype;

proto.connect = function(){
	return P.bind(this)
	.then(function(){
		if(!!this.db){
			this.connId = this.db.connect();
		}
		else{
			return this.client.connect(this.host, this.port);
		}
	})
	.then(function(){
		if(!this.db){ // standalone mode
			this.setIdleTimeout();
		}

		var self = this;
		_connMethods.concat(_collMethods).forEach(function(method){
			self[method] = function(){
				var args = [].slice.call(arguments);
				if(!!self.db){ // in-process mode
					return self.db.execute(self.connId, method, args);
				}
				else{ // standalone mode
					self.setIdleTimeout();
					return self.client.request(method, args);
				}
			};
		});
	});
};

proto.close = function(){
	return P.bind(this)
	.then(function(){
		if(!!this.db){
			return this.db.disconnect(this.connId);
		}
		else{
			return this.client.disconnect();
		}
	})
	.then(function(){
		this.emit('close');
	});
};

proto.collection = function(name){
	var self = this;
	if(!this.collections[name]){
		var collection = {};

		_collMethods.forEach(function(method){
			collection[method] = function(){
				var args = [name].concat([].slice.call(arguments));
				return self[method].apply(self, args);
			};
		});

		this.collections[name] = collection;
	}
	return this.collections[name];
};

proto.setIdleTimeout = function(){
	clearTimeout(this.idleTimeout);
	if(this.idleTimeoutValue){
		this.idleTimeout = setTimeout(this.close.bind(this), this.idleTimeoutValue);
	}
};

module.exports = Connection;
