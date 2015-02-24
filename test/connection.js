'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var env = require('./env');
var Shard = require('../lib/shard');
var Connection = require('../lib/connection');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('connection test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('find/update/insert/remove/commit/rollback', function(cb){
		var shard = new Shard({
			_id : 's1',
			redisConfig : env.redisConfig,
			backend : 'mongodb',
			backendConfig : env.mongoConfig,
		});
		var conn = new Connection({_id : 'c1', shard : shard});
		var user = conn.collection('user');
		var user1 = {_id : 1, name : 'rain', age : 30};
		var user2 = {_id : 2, name : 'tina', age : 24};
		var news = conn.collection('news');
		var news1 = {_id : 1, text : 'hello'};

		return Q.fcall(function(){
			return shard.start();
		})
		.then(function(){
			return user.insert(user1._id, user1);
		})
		.then(function(){
			return user.insert(user2._id, user2);
		})
		.then(function(){
			return conn.commit();
		})
		.then(function(){
			return user.update(user1._id, {age : 31});
		})
		.then(function(){
			return user.find(user1._id, 'age')
			.then(function(ret){
				ret.age.should.eql(31);
			});
		})
		.then(function(){
			return user.find(user2._id)
			.then(function(ret){
				ret.should.eql(user2);
			});
		})
		.then(function(){
			return user.remove(user2._id);
		})
		.then(function(){
			return news.insert(news1._id, news1);
		})
		.then(function(){
			return user.find(user2._id)
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			return conn.rollback();
		})
		.then(function(){
			return user.find(user1._id)
			.then(function(ret){
				ret.should.eql(user1);
			});
		})
		.then(function(){
			return user.find(user2._id)
			.then(function(ret){
				ret.should.eql(user2);
			});
		})
		.then(function(){
			return news.find(news1._id)
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			return user.remove(user1._id);
		})
		.then(function(){
			return user.remove(user2._id);
		})
		.then(function(){
			return conn.commit();
		})
		.then(function(){
			return conn.close();
		})
		.then(function(){
			return shard.stop();
		})
		.done(function(){
			cb();
		});
	});
});
