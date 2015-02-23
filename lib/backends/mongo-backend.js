'use strict';

var Q = require('q');
var mongodb = require('mongodb');

var MongoBackend = function(opts){
	opts = opts || {};
	this._uri = opts.uri || 'mongodb://localhost';
	this._options = opts.options || {};
};

var proto = MongoBackend.prototype;

proto.start = function(){
	var self = this;
	return Q.nfcall(function(cb){
		mongodb.MongoClient.connect(self._uri, self._options, cb);
	}).then(function(ret){
		self.db = ret;
	});
};

proto.stop = function(){
	var self = this;
	return Q.nfcall(function(cb){
		self.db.close(cb);
	});
};

proto.get = function(name, id){
	var self = this;
	return Q.nfcall(function(cb){
		self.db.collection(name).findOne({_id : id}, cb);
	});
};

proto.set = function(name, id, doc){
	var self = this;
	doc._id = id;
	return Q.nfcall(function(cb){
		self.db.collection(name).update({_id : id}, doc, {upsert : true}, cb);
	});
};

proto.del = function(name, id){
	var self = this;
	return Q.nfcall(function(cb){
		self.db.collection(name).remove({_id : id}, cb);
	});
};

// drop table
proto.drop = function(name){
	var self = this;
	return Q.nfcall(function(cb){
		self.db.collection(name).drop(cb);
	}).catch(function(e){
		//ignore error
	});
};

module.exports = MongoBackend;
