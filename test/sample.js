'use strict';

var memorydb = require('../lib');
var Q = require('q');
var should = require('should');

// You can run this on multiple servers, each instance will be a shard in the cluster
var main = function(){

	// memorydb's config
	var config = {
		//shard Id (optional)
		_id : 'shard1',
		// Center backend storage, must be same for shards in the same cluster
		backend : 'mongodb',
		backendConfig : {uri : 'mongodb://localhost/memorydb-test'},
		// Used for backendLock, must be same for shards in the same cluster
		redisConfig : {host : '127.0.0.1', port : 6379},
	};

	var doc = {_id : 1, name : 'rain', level : 1};

	var autoconn = null;
	return Q.fcall(function(){
		// Start memorydb
		return memorydb.start(config);
	})
	.then(function(){
		// Get autoConnection
		autoconn = memorydb.autoConnect();

		return autoconn.execute(function(){
			// Get collection
			var User = autoconn.collection('user');
			return Q.fcall(function(){
				// Insert a doc
				return User.insert(doc._id, doc);
			})
			.then(function(){
				// find the doc
				return User.find(doc._id)
				.then(function(ret){
					ret.should.eql(doc);
				});
			});
		}); // Auto commit here
	})
	.then(function(){
		return autoconn.execute(function(){
			var User = autoconn.collection('user');
			return Q.fcall(function(){
				// Update one field
				return User.update(doc._id, {level : 2});
			}).then(function(){
				// Find specified field
				return User.find(doc._id, 'level')
				.then(function(ret){
					ret.should.eql({level : 2});
				});
			}).then(function(){
				// Exception here!
				throw new Error('Oops!');
			});
		}) // Rollback on exception
		.catch(function(e){
			e.message.should.eql('Oops!');
		});
	})
	.then(function(){
		return autoconn.execute(function(){
			var User = autoconn.collection('user');
			return Q.fcall(function(){
				// doc should be rolled back
				return User.find(doc._id, 'level')
				.then(function(ret){
					ret.should.eql({level : 1});
				});
			})
			.then(function(){
				// Remove the doc
				return User.remove(doc._id);
			}); // Auto commit here
		});
	})
	.then(function(){
		// Close autoConnection
		return autoconn.close();
	})
	.then(function(){
		// Stop memorydb
		return memorydb.stop();
	});
};

if (require.main === module) {
	return Q.fcall(function(){
		return main();
	})
	.fin(function(){
		process.exit();
	});
}
