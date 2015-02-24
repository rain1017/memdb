'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var memorydb = require('../lib');

var main = function(){
	var opts = {
		_id : 's1',
		redisConfig : {host : '127.0.0.1', port : 6379},
		backend : 'mongodb',
		backendConfig : {uri : 'mongodb://localhost/memorydb-test'},
	};

	var autoconn = null;
	var user1 = {_id : 1, name : 'rain', level : 0};

	return Q.fcall(function(){
		return memorydb.start(opts);
	})
	.then(function(){
		autoconn = memorydb.autoConnect();

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
					return User.lock(user1._id);
				})
				.then(function(){
					return User.find(user1._id, 'level');
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
		// You can also use connection directly
		// Make sure the conn is used only by one api request
		var conn = memorydb.connect();
		return Q.fcall(function(){
			return conn.collection('user').find(user1._id);
		})
		.then(function(ret){
			(ret === null).should.eql(true);
		})
		.fin(function(){
			conn.close();
		});
	})
	.then(function(){
		return autoconn.close();
	})
	.then(function(){
		return memorydb.stop();
	})
	.fin(function(){
		process.exit();
	});
};

if (require.main === module) {
    main();
}
