'use strict';

var Q = require('q');
var path = require('path');
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

var MongooseBackend = function(opts){
	opts = opts || {};
	this._uri = opts.uri || 'mongodb://localhost';
	this._options = opts.options || {};
};

var proto = MongooseBackend.prototype;

proto.start = function(){
	var mongoose = require('../mdbgoose');

	var self = this;

	return Q.nfcall(function(cb){
		mongoose.connect(self._uri, self._options, cb);
	})
	.then(function(){
		self.db = mongoose.connection;
		logger.debug('mongoose connected: %s', self._uri);
		Object.defineProperty(self, 'connection', {
			get : function(){
				return self.db;
			}
		});
	});
};

proto.stop = function(){
	var self = this;
	return Q.nfcall(function(cb){
		require('mongoose').disconnect(cb);
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
	if(doc !== null && doc !== undefined){
		doc._id = id;
		return Q.nfcall(function(cb){
			self.db.collection(name).updateMongo({_id : id}, doc, {upsert : true}, cb);
		});
	}
	else{
		return Q.nfcall(function(cb){
			self.db.collection(name).removeMongo({_id : id}, cb);
		});
	}
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
	var self = this;
	return Q.all(items.map(function(item){
		return Q.fcall(function(){
			return self.set(item.name, item.id, item.doc);
		});
	}));
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

module.exports = MongooseBackend;
