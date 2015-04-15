'use strict';

var P = require('bluebird');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Client = require('./client');
var Collection = require('./collection');
var logger = require('pomelo-logger').getLogger('memdb-client', __filename);

var _methods = ['commit', 'rollback', 'insert', 'remove', 'find', 'findForUpdate',
				'update', 'lock', 'findByIndex', 'findCached', 'persistentAll'];

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
		this.setIdleTimeout();

		var self = this;
		_methods.forEach(function(method){
			self[method] = function(){
				if(!!self.db){ // In-process mode
					var args = [self.connId].concat([].slice.call(arguments));
					return self.db[method].apply(self.db, args);
				}
				else{ // socket.io server mode
					self.setIdleTimeout();
					var args = [method].concat([].slice.call(arguments)); //jshint ignore:line
					return self.client.request.apply(self.client, args);
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
	if(!this.collections[name]){
		var collection = new Collection({
			name : name,
			connection : this,
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
