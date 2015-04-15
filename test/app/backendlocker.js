'use strict';

var P = require('bluebird');
var should = require('should');
var logger = require('pomelo-logger').getLogger('test', __filename);
var BackendLocker = require('../../app/backendlocker');
var env = require('../env');

describe('backendlocker test', function(){

	it('lock/unlock', function(cb){
		var locker = new BackendLocker({
							host : env.config.redis.host,
							port : env.config.redis.port,
							});

		var docId = 'doc1', shardId = 'shard1';
		return P.try(function(){
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
		.then(function(){
			return locker.getHolderIdMulti([docId, 'nonExistDoc'])
			.then(function(ret){
				ret.should.eql([shardId, null]);
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
			return locker.unlock(docId);
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
		.finally(function(){
			return P.try(function(){
				return locker.unlockAll();
			}).then(function(){
				locker.close();
			});
		})
		.nodeify(cb);
	});
});
