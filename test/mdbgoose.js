'use strict';

var Q = require('q');
var util = require('util');
var should = require('should');
var env = require('./env');
var mdbgoose = require('../lib/mdbgoose');
var Schema = mdbgoose.Schema;
var types = mdbgoose.SchemaTypes;
var mdb = require('../lib');
var logger = require('pomelo-logger').getLogger('test', __filename);

var playerSchema = new Schema({
	_id : String,
	areaId : String,
	name : String,
	fullname : {first: String, second: String},
	extra : types.Mixed,
}, {collection : 'player'});

var Player = mdbgoose.model('player', playerSchema);

describe('mdbgoose test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('mdbgoose', function(cb){
		var config = env.dbConfig('s1');
		config.collections = {
			'player' : {
				'indexes' : ['areaId'],
			}
		};
		config.backend = 'mongoose';
		var autoconn = null;

		return Q.fcall(function(){
			return mdb.start(config);
		})
		.then(function(){
			autoconn = mdb.autoConnect();
		})
		.then(function(){
			return autoconn.execute(function(){
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
				return Q.fcall(function(){
					return Q.ninvoke(player1, 'save');
				})
				.then(function(){
					return Q.ninvoke(Player, 'findOneInMdb', 'p1');
				})
				.then(function(player){
					logger.debug('%j', player);
					player.name.should.eql('rain');

					player.name = 'snow';
					player.areaId = 'a1';
					player.fullname.first = 'changed first';
					player.extra = {xx1 : 'changed extra'};
					return Q.ninvoke(player, 'save');
				})
				.then(function(){
					return Q.ninvoke(Player, 'findOneInMdb', 'p1')
					.then(function(player){
						logger.debug('%j', player);
						player.name.should.eql('snow');
					});
				})
				.then(function(){
					return Q.ninvoke(player2, 'save');
				})
				.then(function(){
					return Q.ninvoke(Player, 'findByIndexInMdb', 'areaId', 'a1')
					.then(function(players){
						logger.debug('%j', players);
						players.length.should.eql(2);
						players.forEach(function(player){
							player.areaId.should.eql('a1');
						});
					});
				});
			});
		})
		.then(function(){
			// flush all to backend mongodb
			return mdb.persistentAll();
		})
		.then(function(){
			// Call mongodb directly
			return Q.ninvoke(Player, 'find');
		})
		.then(function(players){
			logger.debug('%j', players);
			players.length.should.eql(2);

			return Q.ninvoke(players[0], 'save')
			.fail(function(e){
				logger.warn(e); // should throw error
			});
		})
		.fin(function(){
			return mdb.stop();
		})
		.nodeify(cb);
	});
});
