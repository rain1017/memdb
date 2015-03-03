'use strict';

var redis = require('redis');
var Q = require('q');
var utils = require('./utils');
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

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
	var redisKey = this._redisKey(key);

	if(Object.keys(fields).length > 0){
		var dct = {};
		for(var field in fields){
			dct[field] = JSON.stringify(fields[field]);
		}
		multi = multi.hmset(redisKey, dct);
	}
	multi = multi.set(redisKey + ':e', exist ? 1 : 0);
	return Q.nfcall(function(cb){
		multi.exec(utils.normalizecb(cb));
	});
};

proto.remove = function(key){
	var multi = this.client.multi();
	var redisKey = this._redisKey(key);
	multi = multi.del(redisKey);
	multi = multi.del(redisKey + ':e');
	return Q.nfcall(function(cb){
		multi.exec(utils.normalizecb(cb));
	});
};

proto.findMulti = function(keys){
	var self = this;
	var multi = this.client.multi();
	keys.forEach(function(key){
		var redisKey = self._redisKey(key);
		multi = multi.hgetall(redisKey);
		multi = multi.get(redisKey + ':e');
	});

	return Q.nfcall(function(cb){
		multi.exec(utils.normalizecb(cb));
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

/**
 * changes - {key : change}
 *		change - {fields : {field : value}, exist : 1}
 */
proto.commit = function(changes){
	var multi = this.client.multi();
	for(var key in changes){
		var redisKey = this._redisKey(key);
		var change = changes[key];
		for(var field in change.fields){
			var value = change.fields[field];
			if(value !== undefined){
				multi = multi.hmset(redisKey, field, JSON.stringify(value));
			}
			else{
				multi = multi.hdel(redisKey, field);
			}
			if(change.exist !== undefined){
				multi = multi.set(redisKey + ':e', change.exist ? 1 : 0);
			}
		}
	}

	return Q.nfcall(function(cb){
		multi.exec(utils.normalizecb(cb));
	});
};

proto._redisKey = function(key){
	return 'bak:' + this.shard._id + ':' + key;
};

module.exports = Slave;
