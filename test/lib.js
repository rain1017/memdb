'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var env = require('./env');
var memorydb = require('../lib');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('lib test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('lib test', function(cb){
		var opts = {
			_id : 's1',
			redisConfig : env.redisConfig,
			backend : 'mongodb',
			backendConfig : env.mongoConfig,
		};

		var user1 = {_id : 1, name : 'rain', level : 0};

		return Q.fcall(function(){
			return memorydb.start(opts);
		})
		.then(function(){
			var autoconn = memorydb.autoConnect();
			return autoconn.execute(function(){
				var User = autoconn.collection('user');
				return User.insert(user1._id, user1);
			})
			.fin(function(){
				autoconn.close();
			});
		})
		.then(function(){
			var conn = memorydb.connect();
			return conn.collection('user').find(user1._id)
			.then(function(ret){
				ret.should.eql(user1);
			})
			.then(function(ret){
				return conn.collection('user').remove(user1._id);
			})
			.fin(function(){
				conn.commit();
				conn.close();
			});
		})
		.then(function(){
			return memorydb.stop();
		})
		.done(function(){
			cb();
		});
	});

	it('auto restart', function(cb){
		var opts = {
			_id : 's1',
			redisConfig : env.redisConfig,
			backend : 'mongodb',
			backendConfig : env.mongoConfig,

			// This will cause shard to suicide
			heartbeatTimeout : 200,
			heartbeatInterval : 30 * 1000,
		};

		return Q.fcall(function(){
			return memorydb.start(opts);
		})
		.delay(500)
		.then(function(){
			return memorydb.stop()
			.catch(function(e){}); //Exception is possible
		})
		.delay(500)
		.done(function(){
			cb();
		});
	});
});
