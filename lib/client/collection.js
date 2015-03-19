'use strict';

var Q = require('q');
var util = require('util');
var logger = require('pomelo-logger').getLogger('memorydb-client', __filename);

var Collection = function(opts){
	this.name = opts.name;
	this.connection = opts.connection;
};

var proto = Collection.prototype;

proto.insert = function(id, doc){
	return this.connection.insert(this.name, id, doc);
};

proto.remove = function(id){
	return this.connection.remove(this.name, id);
};

proto.find = function(id, fields){
	return this.connection.find(this.name, id, fields);
};

proto.findForUpdate = function(id, fields){
	return this.connection.findForUpdate(this.name, id, fields);
};

proto.update = function(id, doc, opts){
	return this.connection.update(this.name, id, doc, opts);
};

proto.lock = function(id){
	return this.connection.lock(this.name, id);
};

proto.findByIndex = function(field, value, fields){
	return this.connection.findByIndex(this.name, field, value, fields);
};

proto.findCached = function(id){
	return this.connection.findCached(this.name, id);
};

module.exports = Collection;
