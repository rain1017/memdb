'use strict';

var Q = require('q');
var should = require('should');
var logger = require('pomelo-logger').getLogger('test', __filename);
var BackendLocker = require('../lib/backendlocker');
var env = require('./env');

describe('backendlocker test', function(){

	it('lock/unlock', function(cb){
		var locker = new BackendLocker({
							host : env.redisConfig.host,
							port : env.redisConfig.port,
							});

		var docId = 'doc1', shardId = 'shard1';
		return Q.fcall(function(){
			return locker.unlockAll();
		})
		.then(function(){
			return locker.lock(docId, shardId);
		})
		.then(function(){
			return locker.getHolderId(docId)
			.then(function(ret){
				ret.should.eql(shardId);
			});
		})
		.then(function(ret){
			return locker.isHeldBy(docId, shardId)
			.then(function(ret){
				ret.should.be.true; //jshint ignore:line
			});
		})
		.then(function(){
			return locker.ensureHeldBy(docId, shardId);
		})
		.then(function(){
			// Can't lock again
			return locker.tryLock(docId, shardId)
			.then(function(ret){
				ret.should.be.false; //jshint ignore:line
			});
		})
		.then(function(){
			return locker.unlock(docId, shardId);
		})
		.then(function(){
			//Should throw error
			return locker.ensureHeldBy(docId, shardId)
			.then(function(){
				throw new Error('Should throw error');
			}, function(e){
				//expected
			});
		})
		.then(function(){
			return locker.tryUnlock(docId, shardId)
			.then(function(ret){
				ret.should.be.false; //jshint ignore:line
			});
		})
		.then(function(){
			return locker.lock(docId, shardId);
		})
		.then(function(){
			// Force unlock
			return locker.unlockForce(docId);
		})
		.then(function(){
			return locker.getHolderId(docId)
			.then(function(ret){
				(ret === null).should.be.true; //jshint ignore:line
			});
		})
		.fin(function(){
			return Q.fcall(function(){
				return locker.unlockAll();
			}).then(function(){
				locker.close();
			});
		})
		.done(function(){
			cb();
		});
	});

	it('autoUnlock', function(cb){
		var autoUnlockTimeout = 50, heartbeatTimeout = 1000;
		var docId = 'doc1', shardId = 'shard1';

		var locker = new BackendLocker({
						host : env.redisConfig.host,
						port : env.redisConfig.port,
						shardHeartbeatTimeout : heartbeatTimeout,
						autoUnlockTimeout : autoUnlockTimeout,
						});

		return Q.fcall(function(){
			locker.unlockAll();
		})
		.then(function(){
			locker.shardHeartbeat(shardId);
			return locker.lock(docId, shardId);
		})
		.then(function(){
			locker.autoUnlock(docId);
			// Call more than once is ok
			locker.autoUnlock(docId);
		})
		.delay(20)
		.then(function(){
			// Should still hold lock
			return locker.ensureHeldBy(docId, shardId);
		})
		.delay(autoUnlockTimeout)
		.then(function(){
			// Timed out, should unlocked
			return locker.getHolderId(docId)
			.then(function(ret){
				(ret === null).should.be.true; //jshint ignore:line
			});
		})
		.then(function(){
			locker.shardHeartbeat(shardId);
			return locker.lock(docId, shardId);
		})
		.delay(heartbeatTimeout + 20)
		.then(function(){
			// Heartbeat timed out, should unlocked immediately
			locker.autoUnlock(docId);
		})
		.delay(20)
		.then(function(){
			return locker.getHolderId(docId)
			.then(function(ret){
				(ret === null).should.be.true; //jshint ignore:line
			});
		})
		.then(function(){
			// Test shardStop
			return Q.fcall(function(){
				return locker.shardHeartbeat(shardId);
			})
			.then(function(){
				return locker.shardStop(shardId);
			});
		})
		.fin(function(){
			return Q.fcall(function(){
				return locker.unlockAll();
			}).then(function(){
				locker.close();
			});
		})
		.done(function(){
			cb();
		});
	});
});
