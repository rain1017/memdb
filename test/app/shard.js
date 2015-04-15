'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var Shard = require('../../app/shard');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('shard test', function(){
	beforeEach(env.flushdb);
	after(env.flushdb);

	it('load/unload/find/update/insert/remove/commit/rollback', function(cb){
		var shard = new Shard(env.dbConfig('s1'));
		var connId = 'c1', key = 'user:1', doc = {_id : '1', name : 'rain', age : 30};

		return P.try(function(){
			return shard.start();
		})
		.then(function(){
			// pre create collection for performance
			return shard.backend.connection.createCollectionAsync('user');
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
			shard.globalEvent.emit('request:' + shard._id, key);
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
				ret.should.eql(doc);
			});
		})
		.then(function(){
			// Already loaded, should return immediately
			shard.find(connId, key).should.eql(doc);

			// load again for write
			return shard.lock(connId, key);
		})
		.then(function(){
			shard.update(connId, key, {age : 31});
			shard.find(connId, key, 'age').age.should.eql(31);

			// rollback to original value
			shard.rollback(connId, key);
			shard.find(connId, key, 'age').age.should.eql(30);

			return shard.lock(connId, key);
		})
		.then(function(){
			shard.remove(connId, key);
			(shard.find(connId, key) === null).should.be.true; // jshint ignore:line
			return shard.commit(connId, key);
		})
		.then(function(){
			// request to unload
			shard.globalEvent.emit('request:' + shard._id, key);
		})
		.delay(100)
		.then(function(){
			// load again
			return shard.find(connId, key)
			.then(function(ret){
				(ret === null).should.be.true; // jshint ignore:line
			});
		})
		.then(function(){
			return shard.stop();
		})
		.nodeify(cb);
	});

	it('backendLock between multiple shards', function(cb){
		var config = env.dbConfig('s1');
		config.backendLockRetryInterval = 500; // This is required for this test
		var shard1 = new Shard(config);
		var shard2 = new Shard(env.dbConfig('s2'));

		var key = 'user:1', doc = {_id : '1', name : 'rain', age : 30};
		return P.try(function(){
			return P.all([shard1.start(), shard2.start()]);
		})
		.then(function(){
			// pre create collection for performance
			return shard1.backend.connection.createCollectionAsync('user');
		})
		.then(function(){
			return P.all([
				// shard1:c1
				P.try(function(){
					// load by shard1
					return shard1.lock('c1', key);
				})
				.delay(100) // wait for shard2 request
				.then(function(){
					// should unloading now (wait for unlock)
					// already locked, so will not block
					return shard1.lock('c1', key);
				}).then(function(){
					shard1.insert('c1', key, doc);
					return shard1.commit('c1', key);
					// unlocked, unloading should continue
				}),

				// shard2:c1
				P.delay(20) // shard1 should load first
				.then(function(){
					// This will block until shard1 unload the key
					return shard2.find('c1', key);
				})
				.then(function(ret){
					ret.should.eql(doc);
					return shard2.lock('c1', key);
				})
				.then(function(){
					shard2.remove('c1', key);
					return shard2.commit('c1', key);
				}),

				// shard1:c2
				P.delay(50)
				.then(function(){
					// since shard1 is unloading, lock will block until unloaded
					// and then will load again (after shard2 unload)
					return shard1.lock('c2', key);
				})
				.then(function(){
					// should read shard2 saved value
					(shard1.find('c2', key) === null).should.be.true; // jshint ignore:line
				})
			]);
		})
		.then(function(){
			return P.all([shard1.stop(), shard2.stop()]);
		})
		.nodeify(cb);
	});

	it('doc idle', function(cb){
		var config = env.dbConfig('s1');
		config.docIdleTimeout = 100;
		var shard = new Shard(config);

		var key = 'user:1';
		return P.try(function(){
			return shard.start();
		})
		.then(function(){
			return shard.find('c1', key);
		})
		.delay(200) //Doc idle timed out, should unloaded
		.then(function(){
			shard._isLoaded(key).should.eql(false);
			return shard.stop();
		})
		.nodeify(cb);
	});

	it('globalEvent register/unregister', function(cb){
		var shard = new Shard(env.dbConfig('s1'));
		return P.try(function(){
			return shard.start();
		})
		.then(function(){
			shard.globalEvent.emit('request:s1', 'player:1');
		})
		.delay(100)
		.then(function(){
			return shard.stop();
		})
		.then(function(){
			shard.globalEvent.emit('request:s1', 'player:1');
		})
		.delay(100)
		.nodeify(cb);
	});

	it('find cached', function(cb){
		var config = env.dbConfig('s1');
		config.docCacheTimeout = 100;
		config.docIdleTimeout = 100;
		var shard = new Shard(config);

		var key = 'user:1', doc = {_id : '1', name : 'rain'};
		return P.try(function(){
			return shard.start();
		})
		.then(function(){
			return shard.lock('c1', key);
		})
		.then(function(){
			return shard.insert('c1', key, doc);
		})
		.then(function(){
			return shard.commit('c1', [key]);
		})
		.then(function(){
			return shard.findCached('c1', key)
			.then(function(ret){
				ret.should.eql(doc);
			});
		})
		.delay(500) // doc unloaded and cache timed out
		.then(function(){
			// read from backend
			return P.try(function(){
				return shard.findCached('c1', key);
			})
			.then(function(ret){
				ret.should.eql(doc);
			});
		})
		.then(function(){
			// get from cache
			return P.try(function(){
				return shard.findCached('c1', key);
			})
			.then(function(ret){
				ret.should.eql(doc);
			});
		})
		.then(function(){
			return shard.stop();
		})
		.nodeify(cb);
	});
});
