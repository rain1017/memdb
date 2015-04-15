'use strict';

var P = require('bluebird');
var redis = P.promisifyAll(require('redis'));

var RedisBackend = function(opts){
	opts = opts || {};
	this._host = opts.host || '127.0.0.1';
	this._port = opts.port || 6379;
	this._options = opts.options || {};
	this.prefix = opts.prefix || '';
};

var proto = RedisBackend.prototype;

proto.start = function(){
	this.client = redis.createClient(this._port, this._host, this._options);

	var self = this;
	Object.defineProperty(self, 'connection', {
		get : function(){
			return self.client;
		}
	});
};

proto.stop = function(){
	this.client.end();
};

proto.get = function(name, id){
	return P.bind(this)
	.then(function(){
		return this.client.hmgetAsync(this.prefix + name, id);
	})
	.then(function(ret){
		ret = ret[0];
		return JSON.parse(ret);
	});
};

// delete when doc is null
proto.set = function(name, id, doc){
	if(doc !== null && doc !== undefined){
		return this.client.hmsetAsync(this.prefix + name, id, JSON.stringify(doc));
	}
	else{
		return this.client.hdelAsync(this.prefix + name, id);
	}
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
	var multi = this.client.multi();

	var self = this;
	items.forEach(function(item){
		if(item.doc !== null && item.doc !== undefined){
			multi = multi.hmset(self.prefix + item.name, item.id, JSON.stringify(item.doc));
		}
		else{
			multi = multi.hdel(self.prefix + item.name, item.id);
		}
	});
	return multi.execAsync();
};

// drop table or database
proto.drop = function(name){
	if(!!name){
		this.client.delAsync(this.prefix + name);
	}
	else{
		this.client.flushdbAsync();
	}
};

module.exports = RedisBackend;
