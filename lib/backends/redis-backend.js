'use strict';

var Q = require('q');
var redis = require('redis');
var utils = require('../utils');

var RedisBackend = function(opts){
	opts = opts || {};
	this._host = opts.host || '127.0.0.1';
	this._port = opts.port || 6379;
	this.prefix = opts.prefix || '';
};

var proto = RedisBackend.prototype;

proto.start = function(){
	this.client = redis.createClient(this._port, this._host);

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
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.hmget(self.prefix + name, id, utils.normalizecb(cb));
	}).then(function(ret){
		ret = ret[0];
		return JSON.parse(ret);
	});
};

// delete when doc is null
proto.set = function(name, id, doc){
	var self = this;
	if(doc !== null && doc !== undefined){
		return Q.nfcall(function(cb){
			return self.client.hmset(self.prefix + name, id, JSON.stringify(doc), utils.normalizecb(cb));
		});
	}
	else{
		return Q.nfcall(function(cb){
			return self.client.hdel(self.prefix + name, id, utils.normalizecb(cb));
		});
	}
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
	var self = this;
	var multi = self.client.multi();
	items.forEach(function(item){
		if(item.doc !== null && item.doc !== undefined){
			multi = multi.hmset(self.prefix + item.name, item.id, JSON.stringify(item.doc));
		}
		else{
			multi = multi.hdel(self.prefix + item.name, item.id);
		}
	});
	return Q.nfcall(function(cb){
		return multi.exec(utils.normalizecb(cb));
	});
};

// drop table
proto.drop = function(name){
	var self = this;
	return Q.nfcall(function(cb){
		self.client.del(self.prefix + name, utils.normalizecb(cb));
	});
};

module.exports = RedisBackend;
