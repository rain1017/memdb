'use strict';

var Q = require('q');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Client = require('./client');
var Collection = require('./collection');
var logger = require('pomelo-logger').getLogger('memorydb-client', __filename);

var methods = ['commit', 'rollback', 'insert', 'remove', 'find', 'findForUpdate',
				'update', 'lock', 'findByIndex', 'findCached'];

var DEFAULT_IDLE_TIMEOUT = 0; //never

var Connection = function(opts){
	EventEmitter.call(this);

	opts = opts || {};

	this.config = {};
	this.config.host = opts.host;
	this.config.port = opts.port;
	this.config.idleTimeout = opts.idleTimeout || DEFAULT_IDLE_TIMEOUT;

	this.client = new Client();
	this.collections = {};
	this.idleTimeout = null;
};

util.inherits(Connection, EventEmitter);

var proto = Connection.prototype;

proto.connect = function(){
	var self = this;
	return Q.fcall(function(){
		return self.client.connect(self.config.host, self.config.port);
	})
	.then(function(){
		self.setIdleTimeout();

		methods.forEach(function(method){
			self[method] = function(){
				self.setIdleTimeout();
				var args = [method].concat([].slice.call(arguments));
				return self.client.request.apply(self.client, args);
			};
		});
	});
};

proto.close = function(){
	var self = this;
	return Q.fcall(function(){
		return self.client.disconnect();
	})
	.then(function(){
		self.emit('close');
	});
};

proto.collection = function(name){
	var self = this;
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
	if(this.config.idleTimeout){
		this.idleTimeout = setTimeout(this.close.bind(this), this.config.idleTimeout);
	}
};

module.exports = Connection;
