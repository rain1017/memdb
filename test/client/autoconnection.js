'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var Database = require('../../lib/database');
var AutoConnection = require('../../lib/client/autoconnection');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('autoconnection test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('concurrent execute', function(cb){
		var database = new Database(env.dbConfig('s1'));

		var autoconn = new AutoConnection(database);
		var user1 = {_id : 1, name : 'rain', level : 0};

		return Q.fcall(function(){
			return database.start();
		})
		.then(function(){
			return autoconn.execute(function(){
				var User = autoconn.collection('user');
				return User.insert(user1._id, user1);
			});
		})
		.then(function(){
			var concurrency = 8;

			return Q.all(_.range(concurrency).map(function(){

				// Simulate non-atomic check and update operation
				// each 'thread' add 1 to user1.level
				return autoconn.execute(function(){
					var User = autoconn.collection('user');
					var level = null;

					return Q() // jshint ignore:line
					.delay(_.random(10))
					.then(function(){
						return User.findForUpdate(user1._id, 'level');
					})
					.then(function(ret){
						level = ret.level;
					})
					.delay(_.random(20))
					.then(function(){
						return User.update(user1._id, {level : level + 1});
					});
				});

			}))
			.then(function(){

				return autoconn.execute(function(){
					var User = autoconn.collection('user');
					return Q.fcall(function(){
						return User.find(user1._id);
					})
					.then(function(ret){
						// level should equal to concurrency
						ret.level.should.eql(concurrency);
						return User.remove(user1._id);
					});
				});
			});
		})
		.then(function(){
			return autoconn.execute(function(){
				return Q.fcall(function(){
					var User = autoconn.collection('user');
					return User.insert(user1._id, user1);
				}).then(function(){
					//Should roll back on exception
					throw new Error('Oops!');
				});
			})
			.catch(function(e){
				e.message.should.eql('Oops!');
			});
		})
		.then(function(){
			return autoconn.execute(function(){
				return Q.fcall(function(){
					var User = autoconn.collection('user');
					return User.find(user1._id);
				})
				.then(function(ret){
					(ret === null).should.eql(true);
				});
			});
		})
		.then(function(){
			return autoconn.close();
		})
		.then(function(){
			return database.stop();
		})
		.nodeify(cb);
	});

	it('reentrant lock', function(cb){
		var db = new Database(env.dbConfig('s1'));
		var autoconn = new AutoConnection(db);

		return Q.fcall(function(){
			return db.start();
		})
		.then(function(){
			return autoconn.execute(function(){
				var player = autoconn.collection('player');
				return player.findForUpdate('p1')
				.then(function(){
					return player.findForUpdate('p1');
				});
			});
		})
		.fin(function(){
			return db.stop();
		})
		.nodeify(cb);
	});
});
