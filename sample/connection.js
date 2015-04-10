'use strict';

var memorydb = require('../lib');
var Q = require('q');
var should = require('should');

// For distributed system, just run memorydb in each server with the same config, and each server will be a shard.
var main = function(){

	// memorydb's config
	var config = {
		//shard Id
		shard : 'shard1',
		// Center backend storage, must be same for shards in the same cluster
		backend : {engine : 'mongodb', url : 'mongodb://localhost/memorydb-test'},
		// Used for backendLock, must be same for shards in the same cluster
		redis : {host : '127.0.0.1', port : 6379},
		// For data replication
		slave : {host : '127.0.0.1', port : 6379},
	};

	var player = {_id : 'p1', name : 'rain', level : 1};

	var conn = null;
	return Q.fcall(function(){
		// Start memorydb
		return memorydb.startServer(config);
	})
	.then(function(){
		return memorydb.connect();
	})
	.then(function(ret){
		conn = ret;

		return conn.collection('player').insert(player._id, player);
	})
	.then(function(){
		return conn.collection('player').find(player._id)
		.then(function(ret){
			ret.should.eql(player);
		});
	})
	.then(function(){
		return conn.commit();
	})
	.then(function(){
		return conn.collection('player').update(player._id, {level : 2});
	})
	.then(function(){
		return conn.collection('player').find(player._id, 'level')
		.then(function(ret){
			ret.level.should.eql(2);
		});
	})
	.then(function(){
		return conn.rollback();
	})
	.then(function(){
		return conn.collection('player').find(player._id, 'level')
		.then(function(ret){
			ret.level.should.eql(1); // Rolled back to 1
		});
	})
	.then(function(){
		return conn.collection('player').remove(player._id);
	})
	.then(function(){
		return conn.commit();
	})
	.then(function(){
		return conn.collection('player').find(player._id)
		.then(function(ret){
			(ret === null).should.eql(true);
		});
	})
	.then(function(){
		return conn.close();
	})
	.fin(function(){
		// Stop memorydb
		return memorydb.stopServer();
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
