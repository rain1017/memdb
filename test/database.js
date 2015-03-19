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
		var db = new Database(env.dbConfig('s1'));
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
			return db.findForUpdate(connId, 'user', user1._id)
			.then(function(ret){
				(ret === null).should.eql(true);
			});
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
		var dbConfig = env.dbConfig('s1');
		dbConfig.collections = collectionDefs;
		var db = new Database(dbConfig);

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

	it('restore from slave', function(cb){
		var db1 = null, db2 = null;
		var connId = null;
		var player1 = {_id : 'p1', name : 'rain', age: 30};
		var player2 = {_id : 'p2', name : 'snow', age: 25};

		return Q.fcall(function(){
			db1 = new Database(env.dbConfig('s1'));
			return db1.start();
		})
		.then(function(){
			connId = db1.connect();
		})
		.then(function(){
			return db1.insert(connId, 'player', player1._id, player1);
		})
		.then(function(){
			return db1.insert(connId, 'player', player2._id, player2);
		})
		.then(function(){
			return db1.commit(connId);
		})
		.then(function(){
			db1.shard.state = 4; // Db is suddenly stopped
		})
		.then(function(){
			//restart db
			db2 = new Database(env.dbConfig('s1'));
			return db2.start();
		})
		.then(function(){
			connId = db2.connect();
		})
		.then(function(){
			return db2.find(connId, 'player', player1._id)
			.then(function(ret){
				ret.should.eql(player1);
			});
		})
		.then(function(){
			return db2.find(connId, 'player', player2._id)
			.then(function(ret){
				ret.should.eql(player2);
			});
		})
		.then(function(){
			// test persistent all
			return db2.persistentAll();
		})
		.then(function(){
			return db2.stop();
		})
		.fin(function(){
			// clean up
			db1.shard.state = 2;
			return db1.stop(true);
		})
		.nodeify(cb);
	});
});
