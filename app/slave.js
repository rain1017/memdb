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

proto.insert = function(key, fields, exist){
	var multi = this.client.multi();

	var fieldsKey = this._fieldsKey(key);
	var existKey = this._existKey(key);

	if(Object.keys(fields).length > 0){
		var dct = {};
		for(var field in fields){
			dct[field] = JSON.stringify(fields[field]);
		}
		multi = multi.hmset(fieldsKey, dct);
	}
	multi = multi.set(existKey, exist ? 1 : 0);
	return multi.execAsync();
};

proto.remove = function(key){
	var multi = this.client.multi();
	var fieldsKey = this._fieldsKey(key);
	var existKey = this._existKey(key);
	multi = multi.del(fieldsKey);
	multi = multi.del(existKey);
	return multi.execAsync();
};

proto.findMulti = function(keys){
	var self = this;
	var multi = this.client.multi();
	keys.forEach(function(key){
		var fieldsKey = self._fieldsKey(key);
		var existKey = self._existKey(key);
		multi = multi.hgetall(fieldsKey);
		multi = multi.get(existKey);
	});

	return P.bind(this)
	.then(function(){
		return multi.execAsync();
	})
	.then(function(results){
		// results.length === keys.length * 2
		var docs = {};
		for(var i in keys){
			var key = keys[i];
			var exist = results[i * 2 + 1];
			if(exist === null){
				docs[key] = null; // the doc not in redis
			}
			else{
				var dct = {};
				var fields = results[i * 2] || {};
				for(var field in fields){
					dct[field] = JSON.parse(fields[field]);
				}
				docs[keys[i]] = {
					fields : dct,
					exist : exist === '0' ? false : true,
				};
			}
		}
		return docs;
	});
};

proto.getAllKeys = function(){
	return P.bind(this)
	.then(function(){
		return this.client.keysAsync(this._allKeysPattern());
	})
	.then(function(existKeys){
		var self = this;
		return existKeys.map(function(existKey){
			return self._extractKey(existKey);
		});
	});
};

/**
 * changes - {key : change}
 *		change - {fields : {field : value}, exist : 1}
 */
proto.commit = function(changes){
	var multi = this.client.multi();
	for(var key in changes){
		var fieldsKey = this._fieldsKey(key);
		var existKey = this._existKey(key);
		var change = changes[key];
		for(var field in change.fields){
			var value = change.fields[field];
			if(value !== undefined){
				multi = multi.hmset(fieldsKey, field, JSON.stringify(value));
			}
			else{
				multi = multi.hdel(fieldsKey, field);
			}
			if(change.exist !== undefined){
				multi = multi.set(existKey, change.exist ? 1 : 0);
			}
		}
	}

	return multi.execAsync();
};

// Clear all data in this shard
proto.clear = function(){
	return P.bind(this)
	.then(function(){
		return this.client.keysAsync(this._allPrefix() + '*');
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
 * bak:shardId:f:key -> {field : value} //doc.fields
 * bak:shardId:e:key -> 1 or 0 //doc.exist
 */

proto._fieldsKey = function(key){
	return 'bak:' + this.shard._id + ':f:' + key;
};

proto._existKey = function(key){
	return 'bak:' + this.shard._id + ':e:' + key;
};

proto._allKeysPattern = function(){
	return 'bak:' + this.shard._id + ':e:*';
};

proto._allPrefix = function(){
	return 'bak:' + this.shard._id + ':';
};

proto._extractKey = function(existKey){
	var words = existKey.split(':');
	return words.slice(3, words.length).join(':');
};

module.exports = Slave;
