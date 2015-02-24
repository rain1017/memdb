'use strict';

var Q = require('q');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
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
	}).then(function(ret){
		logger.debug('connection[%s].collection[%s].insert(%s, %s) => %s', self.connection._id, self.name, id, util.inspect(doc), ret);
		return ret;
	});
};

proto.remove = function(id){
	var self = this;
	return Q.fcall(function(){
		return self.lock(id);
	}).then(function(){
		return self.shard.remove(self.connection._id, self._key(id));
	}).then(function(ret){
		logger.debug('connection[%s].collection[%s].remove(%s) => %s', self.connection._id, self.name, id, ret);
		return ret;
	});
};

proto.find = function(id, fields){
	var self = this;
	return Q.fcall(function(){
		return self.shard.find(self.connection._id, self._key(id), fields);
	}).then(function(ret){
		logger.debug('connection[%s].collection[%s].find(%s, %s) => %s', self.connection._id, self.name, id, fields, util.inspect(ret));
		return ret;
	});
};

proto.update = function(id, doc, opts){
	var self = this;
	return Q.fcall(function(){
		return self.lock(id);
	}).then(function(){
		return self.shard.update(self.connection._id, self._key(id), doc, opts);
	}).then(function(ret){
		logger.debug('connection[%s].collection[%s].update(%s, %s, %s) => %s', self.connection._id, self.name, id, util.inspect(doc), opts, ret);
		return ret;
	});
};

proto.lock = function(id){
	var self = this;
	return Q.fcall(function(){
		return self.shard.lock(self.connection._id, self._key(id));
	}).then(function(ret){
		self.emit('lock', id);
		logger.debug('connection[%s].collection[%s].lock(%s) => %s', self.connection._id, self.name, id, ret);
		return ret;
	});
};

proto._key = function(id){
	return this.name + ':' + id;
};

module.exports = Collection;
