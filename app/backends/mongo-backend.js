'use strict';

var Q = global.MEMORYDB_Q || require('q');
var mongodb = require('mongodb');

var MongoBackend = function(opts){
	opts = opts || {};
	this._url = opts.url || 'mongodb://localhost/memorydb';
	this._options = opts.options || {};
};

var proto = MongoBackend.prototype;

proto.start = function(){
	var self = this;
	return Q.nfcall(function(cb){
		mongodb.MongoClient.connect(self._url, self._options, cb);
	}).then(function(ret){
		self.conn = ret;

		Object.defineProperty(self, 'connection', {
			get : function(){
				return self.conn;
			}
		});
	});
};

proto.stop = function(){
	var self = this;
	return Q.nfcall(function(cb){
		self.conn.close(cb);
	});
};

proto.get = function(name, id){
	var self = this;
	return Q.nfcall(function(cb){
		self.conn.collection(name).findOne({_id : id}, cb);
	});
};

proto.set = function(name, id, doc){
	var self = this;
	if(doc !== null && doc !== undefined){
		doc._id = id;
		return Q.nfcall(function(cb){
			self.conn.collection(name).update({_id : id}, doc, {upsert : true}, cb);
		});
	}
	else{
		return Q.nfcall(function(cb){
			self.conn.collection(name).remove({_id : id}, cb);
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

// drop table or database
proto.drop = function(name){
	var self = this;
	if(!!name){
		return Q.ninvoke(self.conn.collection(name), 'drop');
	}
	else{
		return Q.ninvoke(self.conn, 'dropDatabase');
	}
};

module.exports = MongoBackend;
