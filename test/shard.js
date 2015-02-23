'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var env = require('./env');
var Shard = require('../lib/shard');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('shard test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('load/unload', function(cb){
		var shard = new Shard({
			_id : 's1',
			redisConfig : env.redisConfig,
			backend : 'mongodb',
			backendConfig : env.mongoConfig,
		});
		var connId = 'c1', key = 'user:1', doc = {name : 'rain', age : 30};

		return Q.fcall(function(){
			return shard.start();
		})
		.then(function(){
			// pre create collection for performance
			return Q.ninvoke(shard.backend.connection, 'createCollection', 'user');
		})
		.then(function(){
			// should auto load
			return shard.lock(connId, key);
		})
		.then(function(){
			shard._isLoaded(key).should.eql(true);

			// insert doc
			return shard.insert(connId, key, doc);
		})
		.then(function(){
			// request to unload doc
			shard.emit('request:' + key);
		})
		.delay(20)
		.then(function(){
			// should still loaded, waiting for commit
			shard._isLoaded(key).should.eql(true);

			return shard.commit(connId, key);
		})
		.delay(100)
		.then(function(){
			// should unloaded now and saved to backend
			shard._isLoaded(key).should.eql(false);

			// auto load again
			return shard.find(connId, key)
			.then(function(ret){
				// should read saved data
				delete ret._id;
				ret.should.eql(doc);
			});
		})
		.then(function(){
			return shard.stop();
		})
		.done(function(){
			cb();
		});
	});
});
