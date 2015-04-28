'use strict';

var P = require('bluebird');
var mongodb = P.promisifyAll(require('mongodb'));

var MongoBackend = function(opts){
	opts = opts || {};
	this._url = opts.url || 'mongodb://localhost/memdb';
	this._options = opts.options || {};
};

var proto = MongoBackend.prototype;

proto.start = function(){
	return P.bind(this)
	.then(function(){
		return P.promisify(mongodb.MongoClient.connect)(this._url, this._options);
	})
	.then(function(ret){
		this.conn = ret;

		Object.defineProperty(this, 'connection', {
			get : function(){
				return ret;
			}
		});
	});
};

proto.stop = function(){
	return this.conn.closeAsync();
};

proto.get = function(name, id){
	return this.conn.collection(name).findOneAsync({_id : id});
};

proto.set = function(name, id, doc){
	if(doc !== null && doc !== undefined){
		return this.conn.collection(name).updateAsync({_id : id}, doc, {upsert : true});
	}
	else{
		return this.conn.collection(name).removeAsync({_id : id});
	}
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
	return P.bind(this)
	.then(function(){
		return items;
	})
	.map(function(item){
		return this.set(item.name, item.id, item.doc);
	});
};

// drop table or database
proto.drop = function(name){
	if(!!name){
		return this.conn.collection(name).dropAsync();
	}
	else{
		return this.conn.dropDatabaseAsync();
	}
};

module.exports = MongoBackend;
