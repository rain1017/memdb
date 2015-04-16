'use strict';

var P = require('bluebird');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Collection = require('./collection');
var Connection = require('./connection');
var Shard = require('./shard');
var logger = require('pomelo-logger').getLogger('memdb', __filename);

/**
 * opts.collections - {name : definition}
 * 		definition.indexes - [field, field]
 */
var Database = function(opts){
	opts = opts || {};
	this.shard = new Shard(opts);
	this.collections = {};
	this.connections = {};
	this.connectionAutoId = 1;

	opts.collections = opts.collections || {};

	this.config = opts;
};

util.inherits(Database, EventEmitter);

var proto = Database.prototype;

proto.start = function(){
	return this.shard.start();
};

proto.stop = function(force){
	return this.shard.stop(force);
};

proto.connect = function(){
	var conn = new Connection({_id : this.connectionAutoId++});
	this.connections[conn._id] = conn;
	logger.info('shard[%s] connection[%s] created', this.shard._id, conn._id);
	return conn._id;
};

proto.disconnect = function(connId){
	var conn = this._connection(connId);
	if(conn.isDirty()){
		this.rollback(connId);
	}
	delete this.connections[connId];
	logger.info('shard[%s] connection[%s] closed', this.shard._id, connId);
};

proto.insert = function(connId, name, id, doc){
	var conn = this._connection(connId);
	return this._collection(name).insert(connId, id, doc);
};

proto.remove = function(connId, name, id){
	var conn = this._connection(connId);
	return this._collection(name).remove(connId, id);
};

proto.find = function(connId, name, id, fields){
	var conn = this._connection(connId);
	return this._collection(name).find(connId, id, fields);
};

proto.findForUpdate = function(connId, name, id, fields){
	var conn = this._connection(connId);
	return this._collection(name).findForUpdate(connId, id, fields);
};

proto.findCached = function(connId, name, id){
	var conn = this._connection(connId);
	return this._collection(name).findCached(connId, id);
};

proto.update = function(connId, name, id, doc, opts){
	var conn = this._connection(connId);
	return this._collection(name).update(connId, id, doc, opts);
};

proto.lock = function(connId, name, id){
	var conn = this._connection(connId);
	return this._collection(name).lock(connId, id);
};

proto.findByIndex = function(connId, name, field, value, fields){
	var conn = this._connection(connId);
	return this._collection(name).findByIndex(connId, field, value, fields);
};

proto.commit = function(connId){
	var conn = this._connection(connId);
	return P.bind(this)
	.then(function(){
		return this.shard.commit(connId, conn.getLockedKeys());
	})
	.then(function(){
		conn.clearLockedKeys();
		logger.info('shard[%s] connection[%s] commited', this.shard._id, connId);
	});
};

proto.rollback = function(connId){
	var conn = this._connection(connId);

	var self = this;
	conn.getLockedKeys().forEach(function(key){
		self.shard.rollback(connId, key);
	});
	conn.clearLockedKeys();

	logger.info('shard[%s] connection[%s] rolledback', this.shard._id, connId);
};

proto.persistentAll = function(){
	return this.shard.persistentAll();
};

proto._collection = function(name){
	var self = this;
	if(!this.collections[name]){
		var collection = new Collection({
			name : name,
			shard : this.shard,
			db : this,
			config : this.config.collections[name] || {},
		});
		collection.on('lock', function(connId, id){
			var conn = self._connection(connId);
			conn.addLockedKey(name + ':' + id);
		});
		this.collections[name] = collection;
	}
	return this.collections[name];
};

proto._connection = function(id){
	var conn = this.connections[id];
	if(!conn){
		//TODO: This line is triggerred once in autoconnection during test, there may be a bug
		throw new Error('connection ' + id + ' not exist');
	}
	return conn;
};

module.exports = Database;
