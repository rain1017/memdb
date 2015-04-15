'use strict';

var memdb = require('../lib');
var P = require('bluebird');
var should = require('should');

// For distributed system, just run memdb in each server (Each instance is a shard).

var main = function(){

	// memdb's config
	var config = {
		//shard Id (Must unique and immutable for each server)
		shard : 'shard1',
		// Center backend storage, must be same for all shards
		backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
		// Used for backendLock, must be same for all shards
		redis : {host : '127.0.0.1', port : 6379},
		// Redis data replication (for current shard)
		slave : {host : '127.0.0.1', port : 6379},
	};

	var doc = {_id : 1, name : 'rain', level : 1};

	var autoconn = null;
	return P.try(function(){
		// Start memdb
		return memdb.startServer(config);
	})
	.then(function(){
		// Get autoConnection
		autoconn = memdb.autoConnect();

		// One transaction for each execution scope
		return autoconn.execute(function(){
			// Get collection
			var User = autoconn.collection('user');
			return P.try(function(){
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
		// One transaction for each execution scope
		return autoconn.execute(function(){
			// Get collection
			var User = autoconn.collection('user');
			return P.try(function(){
				// Update one field
				return User.update(doc._id, {level : 2});
			})
			.then(function(){
				// Find specified field
				return User.find(doc._id, 'level')
				.then(function(ret){
					ret.should.eql({level : 2});
				});
			})
			.then(function(){
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
			// Get collection
			var User = autoconn.collection('user');
			return P.try(function(){
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
	.finally(function(){
		// Stop memdb
		return memdb.stopServer();
	});
};

if (require.main === module) {
	return P.try(function(){
		return main();
	})
	.finally(function(){
		process.exit();
	});
}
