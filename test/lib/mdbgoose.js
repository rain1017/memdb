'use strict';

var P = require('bluebird');
var util = require('util');
var should = require('should');
var env = require('../env');
var memdb = require('../../lib');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('mdbgoose test', function(){
	beforeEach(env.flushdb);
	after(env.flushdb);

	it('mdbgoose', function(cb){
		var mdbgoose = memdb.goose;
		var Schema = mdbgoose.Schema;
		var types = mdbgoose.SchemaTypes;

		mdbgoose.setConnectOpts({host : env.config.shards.s1.host, port : env.config.shards.s1.port});

		delete mdbgoose.connection.models.player;
		var playerSchema = new Schema({
			_id : String,
			areaId : String,
			name : String,
			fullname : {first: String, second: String},
			extra : types.Mixed,
		}, {collection : 'player', versionKey: false});

		var Player = mdbgoose.model('player', playerSchema);

		var serverProcess = null;

		return P.try(function(){
			return env.startServer('s1');
		})
		.then(function(ret){
			serverProcess = ret;

			// connect to backend mongodb
			return mdbgoose.connectAsync(env.config.backend.url);
		})
		.then(function(){
			return mdbgoose.execute(function(){
				var player1 = new Player({
									_id : 'p1',
									areaId: 'a2',
									name: 'rain',
									fullname: {first: 'first', second: 'second'},
									extra: {xx: 'extra val'},
								});
				var player2 = new Player({
									_id : 'p2',
									areaId: 'a1',
									name: 'snow',
									fullname: {first: 'first', second: 'second'},
									extra: {},
								});
				return P.try(function(){
					return player1.saveAsync();
				})
				.then(function(){
					return Player.findForUpdateAsync('p1');
				})
				.then(function(player){
					logger.debug('%j', player);
					player.name.should.eql('rain');

					player.name = 'snow';
					player.areaId = 'a1';
					player.fullname.first = 'changed first';
					player.fullname.second = 'changed second';
					player.extra = {xx1 : 'changed extra'};

					return player.saveAsync();
				})
				.then(function(){
					return P.try(function(){
						return Player.findAsync('p1');
					})
					.then(function(player){
						logger.debug('%j', player);
						player.name.should.eql('snow');
					});
				})
				.then(function(){
					return player2.saveAsync();
				})
				.then(function(){
					return P.try(function(){
						return Player.findByIndexAsync('areaId', 'a1');
					})
					.then(function(players){
						logger.debug('%j', players);
						players.length.should.eql(2);
						players.forEach(function(player){
							player.areaId.should.eql('a1');
						});
					});
				})
				.then(function(){
					return Player.findCachedAsync('p1')
					.then(function(ret){
						logger.debug('%j', ret);
					});
				});
			});
		})
		.then(function(){
			// force persistent to mongodb
			return mdbgoose.autoConnect().persistentAll();
		})
		.then(function(){
			// Call mongodb directly
			return Player.findMongoAsync();
		})
		.then(function(players){
			logger.debug('%j', players);
			players.length.should.eql(2);

			return players[0].saveAsync()
			.catch(function(e){
				logger.warn(e); // should throw error
			});
		})
		.then(function(){
			return memdb.close();
		})
		.finally(function(){
			return env.stopServer(serverProcess);
		})
		.nodeify(cb);
	});

	it('mdbgoose (in-process mode)', function(cb){
		var mdbgoose = memdb.goose;
		var Schema = mdbgoose.Schema;
		var types = mdbgoose.SchemaTypes;

		mdbgoose.setConnectOpts();

		delete mdbgoose.connection.models.player;
		var playerSchema = new Schema({
			_id : String,
			areaId : String,
			name : String,
			fullname : {first: String, second: String},
			extra : types.Mixed,
		}, {collection : 'player', versionKey: false});

		var Player = mdbgoose.model('player', playerSchema);

		return P.try(function(){
			return memdb.startServer(env.dbConfig('s1'));
		})
		.then(function(){
			return mdbgoose.execute(function(){
				var player1 = new Player({
									_id : 'p1',
									areaId: 'a2',
									name: 'rain',
								});
				return P.try(function(){
					return player1.saveAsync();
				})
				.then(function(){
					return Player.findAsync('p1');
				})
				.then(function(player){
					player.name.should.eql('rain');
					return player.removeAsync();
				});
			});
		})
		.finally(function(){
			return memdb.stopServer();
		})
		.nodeify(cb);
	});
});
