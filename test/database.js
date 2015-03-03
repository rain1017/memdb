'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var env = require('./env');
var Database = require('../lib/database');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('database test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('find/update/insert/remove/commit/rollback', function(cb){
		var db = new Database({
			_id : 's1',
			redisConfig : env.redisConfig,
			backend : 'mongodb',
			backendConfig : env.mongoConfig,
		});
		var connId = null;
		var user1 = {_id : 1, name : 'rain', age : 30};
		var user2 = {_id : 2, name : 'tina', age : 24};
		var news1 = {_id : 1, text : 'hello'};

		return Q.fcall(function(){
			return db.start();
		})
		.then(function(){
			connId = db.connect();
		})
		.then(function(){
			return db.insert(connId, 'user', user1._id, user1);
		})
		.then(function(){
			return db.insert(connId, 'user', user2._id, user2);
		})
		.then(function(){
			return db.commit(connId);
		})
		.then(function(){
			return db.update(connId, 'user', user1._id, {age : 31});
		})
		.then(function(){
			return db.find(connId, 'user', user1._id, 'age')
			.then(function(ret){
				ret.age.should.eql(31);
			});
		})
		.then(function(){
			return db.find(connId, 'user', user2._id)
			.then(function(ret){
				ret.should.eql(user2);
			});
		})
		.then(function(){
			return db.remove(connId, 'user', user2._id);
		})
		.then(function(){
			return db.insert(connId, 'news', news1._id, news1);
		})
		.then(function(){
			return db.find(connId, 'user', user2._id)
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			return db.rollback(connId);
		})
		.then(function(){
			return db.find(connId, 'user', user1._id)
			.then(function(ret){
				ret.should.eql(user1);
			});
		})
		.then(function(){
			return db.find(connId, 'user', user2._id)
			.then(function(ret){
				ret.should.eql(user2);
			});
		})
		.then(function(){
			return db.find(connId, 'news', news1._id)
			.then(function(ret){
				(ret === null).should.eql(true);
			});
		})
		.then(function(){
			return db.remove(connId, 'user', user1._id);
		})
		.then(function(){
			return db.remove(connId, 'user', user2._id);
		})
		.then(function(){
			return db.commit(connId);
		})
		.then(function(){
			db.disconnect(connId);
			return db.stop();
		})
		.nodeify(cb);
	});

	it('index test', function(cb){
		var collectionDefs = {
			'player' : {
				'indexes' : ['areaId'],
			}
		};
		var db = new Database({
			_id : 's1',
			redisConfig : env.redisConfig,
			backend : 'mongodb',
			backendConfig : env.mongoConfig,
			collections : collectionDefs,
		});

		var connId = null;

		return Q.fcall(function(){
			return db.start();
		})
		.then(function(){
			connId = db.connect();
		})
		.then(function(){
			return db.findByIndex(connId, 'player', 'areaId', 1)
			.then(function(players){
				players.length.should.eql(0);
			});
		})
		.then(function(){
			return db.insert(connId, 'player', 1, {areaId : 1});
		})
		.then(function(){
			return db.insert(connId, 'player', 2, {areaId : 1});
		})
		.then(function(){
			return db.findByIndex(connId, 'player', 'areaId', 1)
			.then(function(players){
				players.length.should.eql(2);
				players.forEach(function(player){
					player.areaId.should.eql(1);
				});
			});
		})
		.then(function(){
			return db.update(connId, 'player', 2, {areaId : 2});
		})
		.then(function(){
			return db.findByIndex(connId, 'player', 'areaId', 1)
			.then(function(players){
				players.length.should.eql(1);
				players[0].areaId.should.eql(1);
			});
		})
		.then(function(){
			return db.remove(connId, 'player', 1);
		})
		.then(function(){
			return db.findByIndex(connId, 'player', 'areaId', 1)
			.then(function(players){
				players.length.should.eql(0);
			});
		})
		.then(function(){
			return db.findByIndex(connId, 'player', 'areaId', 2)
			.then(function(players){
				players.length.should.eql(1);
				players[0].areaId.should.eql(2);
			});
		})
		.then(function(){
			db.disconnect(connId);
			return db.stop();
		})
		.nodeify(cb);
	});
});
