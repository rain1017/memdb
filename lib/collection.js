'use strict';

var Q = require('q');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

var Collection = function(opts){
	this.name = opts.name;
	this.shard = opts.shard;
	this.connection = opts.connection;

	EventEmitter.call(this);
};

util.inherits(Collection, EventEmitter);

var proto = Collection.prototype;

proto.insert = function(id, doc){
	var self = this;
	return Q.fcall(function(){
		return self.lock(id);
	}).then(function(){
		return self.shard.insert(self.connection._id, self._key(id), doc);
	});
};

proto.remove = function(id){
	var self = this;
	return Q.fcall(function(){
		return self.lock(id);
	}).then(function(){
		return self.shard.remove(self.connection._id, self._key(id));
	});
};

proto.find = function(id, fields){
	var self = this;
	return self.shard.find(self.connection._id, self._key(id), fields);
};

proto.update = function(id, doc, opts){
	var self = this;
	return Q.fcall(function(){
		return self.lock(id);
	}).then(function(){
		return self.shard.update(self.connection._id, self._key(id), doc, opts);
	});
};

proto.lock = function(id){
	var self = this;
	return Q.fcall(function(){
		return self.shard.lock(self.connection._id, self._key(id));
	}).then(function(){
		self.emit('lock', id);
	});
};

proto._key = function(id){
	return this.name + ':' + id;
};
