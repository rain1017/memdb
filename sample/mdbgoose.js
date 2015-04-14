'use strict';

var memdb = require('../lib');
var Q = require('q');
var should = require('should');

var mdbgoose = memdb.goose;
var Schema = mdbgoose.Schema;

var playerSchema = new Schema({
	_id : String,
	name : String,
	fullname : {first: String, last: String},
	extra : mdbgoose.SchemaTypes.mixed,
}, {collection : 'player', versionKey: false});

var Player = mdbgoose.model('player', playerSchema);

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

	return Q.fcall(function(){
		return memdb.startServer(config);
	})
	.then(function(){
		return mdbgoose.execute(function(){
			return Q.fcall(function(){
				var player = new Player({
					_id : 'p1',
					name: 'rain',
					fullname : {firt : 'Yu', last : 'Xia'},
					extra : {},
				});
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
		return memdb.stopServer();
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
