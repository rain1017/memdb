'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var memdb = require('../../lib');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('connection test', function(){
	beforeEach(env.flushdb);
	after(env.flushdb);

	it('find/update/insert/remove/commit/rollback', function(cb){
		var serverProcess = null, conn = null;
		var User = null, News = null;
		var user1 = {_id : 1, name : 'rain', age : 30};
		var user2 = {_id : 2, name : 'tina', age : 24};
		var news1 = {_id : 1, text : 'hello'};

		return P.try(function(){
			return env.startServer('s1');
		})
		.then(function(ret){
			serverProcess = ret;
			return memdb.connect({host : env.config.shards.s1.host, port : env.config.shards.s1.port});
		})
		.then(function(ret){
			conn = ret;
			User = conn.collection('user');
			News = conn.collection('news');
		})
		.then(function(){
			return User.insert(user1._id, user1);
		})
		.then(function(){
			return User.insert(user2._id, user2);
		})
		.then(function(){
			return conn.commit();
		})
		.then(function(){
			return User.update(user1._id, {age : 31});
		})
		.then(function(){
			return User.find(user1._id, 'age')
			.then(function(ret){
				ret.age.should.eql(31);
			});
		})
		.then(function(){
			return User.find(user2._id)
			.then(function(ret){
				ret.should.eql(user2);
			});
		})
		.then(function(){
			return User.remove(user2._id);
		})
		.then(function(){
			return News.insert(news1._id, news1);
		})
		.then(function(){
			return User.find(user2._id)
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			return conn.rollback();
		})
		.then(function(){
			return User.find(user1._id)
			.then(function(ret){
				ret.should.eql(user1);
			});
		})
		.then(function(){
			return User.find(user2._id)
			.then(function(ret){
				ret.should.eql(user2);
			});
		})
		.then(function(){
			return News.find(news1._id)
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			return User.remove(user1._id);
		})
		.then(function(){
			return User.remove(user2._id);
		})
		.then(function(){
			return conn.commit();
		})
		.then(function(){
			return conn.close();
		})
		.finally(function(){
			return env.stopServer(serverProcess);
		})
		.nodeify(cb);
	});

	it('index test', function(cb){
		var serverProcess = null, conn = null;
		var Player = null;

		return P.try(function(){
			return env.startServer('s1');
		}).then(function(ret){
			serverProcess = ret;
			return memdb.connect({host : env.config.shards.s1.host, port : env.config.shards.s1.port});
		})
		.then(function(ret){
			conn = ret;
			Player = conn.collection('player');
		})
		.then(function(){
			return Player.findByIndex('areaId', 1)
			.then(function(players){
				players.length.should.eql(0);
			});
		})
		.then(function(){
			return Player.insert(1, {areaId : 1});
		})
		.then(function(){
			return Player.insert(2, {areaId : 1});
		})
		.then(function(){
			return Player.findByIndex('areaId', 1)
			.then(function(players){
				players.length.should.eql(2);
				players.forEach(function(player){
					player.areaId.should.eql(1);
				});
			});
		})
		.then(function(){
			return Player.update(2, {areaId : 2});
		})
		.then(function(){
			return Player.findByIndex('areaId', 1)
			.then(function(players){
				players.length.should.eql(1);
				players[0].areaId.should.eql(1);
			});
		})
		.then(function(){
			return Player.remove(1);
		})
		.then(function(){
			return Player.findByIndex('areaId', 1)
			.then(function(players){
				players.length.should.eql(0);
			});
		})
		.then(function(){
			return Player.findByIndex('areaId', 2)
			.then(function(players){
				players.length.should.eql(1);
				players[0].areaId.should.eql(2);
			});
		})
		.then(function(){
			return conn.close();
		})
		.finally(function(){
			return env.stopServer(serverProcess);
		})
		.nodeify(cb);
	});

	it('findCached', function(cb){
		var serverProcess = null, conn = null;

		return P.try(function(){
			return env.startServer('s1');
		}).then(function(ret){
			serverProcess = ret;

			return memdb.connect({host : env.config.shards.s1.host, port : env.config.shards.s1.port});
		})
		.then(function(ret){
			conn = ret;

			// miss cache
			return conn.collection('player').findCached('p1')
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			// hit cache
			return conn.collection('player').findCached('p1')
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			return conn.close();
		})
		.finally(function(){
			return env.stopServer(serverProcess);
		})
		.nodeify(cb);
	});
});
