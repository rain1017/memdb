'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var Database = require('../../app/database');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('database test', function(){
	beforeEach(env.flushdb);
	after(env.flushdb);

	// it('find/update/insert/remove/commit/rollback', function(cb){
	// 	//tested in ../lib/connection
	// });

	// it('index test', function(cb){
	// 	//tested in ../lib/connection
	// });

	it('restore from slave', function(cb){
		var db1 = null, db2 = null;
		var connId = null;
		var player1 = {_id : 'p1', name : 'rain', age: 30};
		var player2 = {_id : 'p2', name : 'snow', age: 25};

		return P.try(function(){
			db1 = new Database(env.dbConfig('s1'));
			return db1.start();
		})
		.then(function(){
			connId = db1.connect();
		})
		.then(function(){
			return db1.insert(connId, 'player', player1);
		})
		.then(function(){
			return db1.insert(connId, 'player', player2);
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
			return P.try(function(){
				return db2.findById(connId, 'player', player1._id);
			})
			.then(function(ret){
				ret.should.eql(player1);
			});
		})
		.then(function(){
			return P.try(function(){
				return db2.findById(connId, 'player', player2._id);
			})
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
		.finally(function(){
			// clean up
			db1.shard.state = 2;
			return db1.stop(true);
		})
		.nodeify(cb);
	});
});
