'use strict';

var Q = require('q');
var Collection = require('./collection');
var util = require('util');
var logger = require('pomelo-logger').getLogger('memorydb-client', __filename);

var DEFAULT_COMMIT_TIMEOUT = 10 * 1000;

var Connection = function(db){
	this.db = db;
	this._id = this.db.connect();
	this.collections = {};
	this.closed = false;
};

var proto = Connection.prototype;

proto.close = function(){
	this._ensureActive();
	this.db.disconnect(this._id);
	this.closed = true;
	logger.info('connection[%s] closed', this._id);
};

proto.collection = function(name){
	this._ensureActive();
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

proto.commit = function(){
	this._ensureActive();
	return this.db.commit(this._id);
};

proto.rollback = function(){
	this._ensureActive();
	return this.db.rollback(this._id);
};

proto.insert = function(name, id, doc){
	this._ensureActive();
	return this.db.insert(this._id, name, id, doc);
};

proto.remove = function(name, id){
	this._ensureActive();
	return this.db.remove(this._id, name, id);
};

proto.find = function(name, id, fields){
	this._ensureActive();
	return this.db.find(this._id, name, id, fields);
};

proto.update = function(name, id, doc, opts){
	this._ensureActive();
	return this.db.update(this._id, name, id, doc, opts);
};

proto.lock = function(name, id){
	this._ensureActive();
	return this.db.lock(this._id, name, id);
};

proto.findByIndex = function(name, field, value, fields){
	this._ensureActive();
	return this.db.findByIndex(this._id, name, field, value, fields);
};

proto._ensureActive = function(){
	if(this.closed){
		throw new Error('Connection is closed');
	}
};

module.exports = Connection;
