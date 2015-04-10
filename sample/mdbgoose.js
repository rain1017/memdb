'use strict';

var memorydb = require('../lib');
var Q = require('q');
var should = require('should');

var mdbgoose = memorydb.goose;
var Schema = mdbgoose.Schema;

var playerSchema = new Schema({
	_id : String,
	areaId : String,
	name : String,
}, {collection : 'player', versionKey: false});

var Player = mdbgoose.model('player', playerSchema);

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

	return Q.fcall(function(){
		return memorydb.startServer(config);
	})
	.then(function(){
		return mdbgoose.execute(function(){
			var player = new Player({
								_id : 'p1',
								areaId: 'a1',
								name: 'rain',
							});
			return Q.fcall(function(){
				return player.saveQ();
			})
			.then(function(){
				return Player.findQ('p1');
			})
			.then(function(player){
				player.name.should.eql('rain');
				return player.removeQ();
			});
		});
	})
	.fin(function(){
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
