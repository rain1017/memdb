'use strict';

var Q = require('q');
var uuid = require('node-uuid');
var Collection = require('./collection');
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

var DEFAULT_COMMIT_TIMEOUT = 10 * 1000;

var Connection = function(opts){
	opts = opts || {};
	this._id = opts._id || uuid.v4();
	this.shard = opts.shard;
	this.lockedDocs = {};
	this.collections = {};

	this.commitTimeoutValue = opts.commitTimeout || DEFAULT_COMMIT_TIMEOUT;
	this.commitTimeout = null;

	this.closed = false;
};

var proto = Connection.prototype;

proto.close = function(){
	this._ensureActive();
	this.rollback();
	this.closed = true;
};

proto.collection = function(name){
	this._ensureActive();

	var self = this;
	if(!this.collections[name]){
		this.collections[name] = new Collection({
			name : name,
			shard : this.shard,
			connection : this,
		});
	}
	var collection = this.collections[name];
	collection.on('lock', function(id){
		self.lockedDocs[name + ':' + id] = true;

		// Auto rollback on commit timeout
		if(!self.commitTimeout){
			self.commitTimeout = setTimeout(self.rollback.bind(self), self.commitTimeoutValue);
		}
	});
	return collection;
};

proto.commit = function(){
	this._ensureActive();

	var self = this;
	Object.keys(self.lockedDocs).forEach(function(id){
		self.shard.commit(self._id, id);
	});
	self.lockedDocs = {};
	clearTimeout(self.commitTimeout);
};

proto.rollback = function(){
	this._ensureActive();

	var self = this;
	Object.keys(self.lockedDocs).forEach(function(id){
		self.shard.rollback(self._id, id);
	});
	self.lockedDocs = {};
	clearTimeout(self.commitTimeout);
};

proto._ensureActive = function(){
	if(this.closed){
		throw new Error('Connection is closed');
	}
};

module.exports = Connection;
