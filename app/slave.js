'use strict';

var P = require('bluebird');
var redis = P.promisifyAll(require('redis'));
var utils = require('./utils');
var logger = require('pomelo-logger').getLogger('memdb', __filename);

var Slave = function(shard, opts){
	opts = opts || {};
	this.shard = shard;

	var host = opts.host || '127.0.0.1';
	var port = opts.port || 6379;
	this.client = redis.createClient(port, host);
};

var proto = Slave.prototype;

proto.start = function(){

};

proto.stop = function(){
	this.client.end();
};

proto.set = function(key, doc){
	return this.client.setAsync(this._redisKey(key), JSON.stringify(doc));
};

proto.del = function(key){
	return this.client.delAsync(this._redisKey(key));
};

proto.setMulti = function(docs){
	var multi = this.client.multi();
	for(var key in docs){
		var doc = docs[key];
		multi = multi.set(this._redisKey(key), JSON.stringify(doc));
	}
	return multi.execAsync();
};

proto.findMulti = function(keys){
	var self = this;
	var multi = this.client.multi();
	keys.forEach(function(key){
		multi = multi.get(self._redisKey(key));
	});

	return P.bind(this)
	.then(function(){
		return multi.execAsync();
	})
	.then(function(results){
		var docs = {};
		for(var i in keys){
			var key = keys[i];
			if(!!results[i]){
				docs[key] = JSON.parse(results[i]);
			}
		}
		return docs;
	});
};

proto.getAllKeys = function(){
	return P.bind(this)
	.then(function(){
		return this.client.keysAsync(this._redisPrefix() + '*');
	})
	.then(function(keys){
		var self = this;
		return keys.map(function(key){
			return self._extractKey(key);
		});
	});
};

// Clear all data in this shard
proto.clear = function(){
	return P.bind(this)
	.then(function(){
		return this.client.keysAsync(this._redisPrefix() + '*');
	})
	.then(function(keys){
		var multi = this.client.multi();
		keys.forEach(function(key){
			multi = multi.del(key);
		});
		return multi.execAsync();
	});
};

/**
 * Redis format
 *
 * bak:shardId:key -> JSON.stringify(doc)
 */

proto._redisPrefix = function(){
	return 'bak:' + this.shard._id + ':';
};

proto._redisKey = function(key){
	return this._redisPrefix() + key;
};

proto._extractKey = function(existKey){
	var words = existKey.split(':');
	return words.slice(2, words.length).join(':');
};

module.exports = Slave;
