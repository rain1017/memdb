'use strict';

var Q = require('q');
var redis = require('redis');

var RedisBackend = function(opts){
	opts = opts || {};
	this._host = opts.host || '127.0.0.1';
	this._port = opts.port || 6379;
};

var proto = RedisBackend.prototype;

proto.start = function(){
	this.client = redis.createClient(this._port, this._host);
};

proto.stop = function(){
	this.client.end();
};

proto.get = function(name, id){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.hmget(name, id, function(err, ret){
			if(err){
				err = new Error(err);
			}
			cb(err, ret);
		});
	}).then(function(ret){
		return JSON.parse(ret);
	});
};

proto.set = function(name, id, doc){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.hmset(name, id, JSON.stringify(doc), function(err, ret){
			if(err){
				err = new Error(err);
			}
			cb(err, ret);
		});
	});
};

proto.del = function(name, id){
	var self = this;
	return Q.nfcall(function(cb){
		return self.client.hdel(name, id, function(err, ret){
			if(err){
				err = new Error(err);
			}
			cb(err, ret);
		});
	});
};

module.exports = RedisBackend;
