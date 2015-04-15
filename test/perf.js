'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('./env');
var memdb = require('../lib');
var Database = require('../app/database');
var AutoConnection = require('../lib/autoconnection');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe.skip('performance test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('write single doc', function(cb){
		this.timeout(30 * 1000);

		var count = 2000;
		var autoconn = null;
		var player = {_id : 1, name : 'rain', exp : 0};

		var incPlayerExp = function(){
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				return P.try(function(){
					return Player.findForUpdate(player._id);
				})
				.then(function(doc){
					doc.exp++;
					return Player.update(player._id, doc);
				});
			})
			.catch(function(e){
				logger.error(e);
			});
		};

		var startTick = null;
		var rate = null;
		return P.try(function(){
			return memdb.startServer(env.dbConfig('s1'));
		})
		.then(function(){
			autoconn = memdb.autoConnect();

			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				return Player.insert(player._id, player);
			});
		})
		.then(function(){
			startTick = Date.now();
			return P.reduce(_.range(count), function(sofar, value){
				return incPlayerExp();
			}, 0);
		})
		.then(function(){
			rate = count * 1000 / (Date.now() - startTick);
			logger.warn('Rate: %s', rate);

			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				return P.try(function(){
					return Player.find(player._id);
				})
				.then(function(ret){
					logger.warn('%j', ret);

					return Player.remove(player._id);
				});
			});
		})
		.finally(function(){
			return memdb.stopServer();
		})
		.nodeify(cb);
	});

	it('write single doc in one transcation', function(cb){
		this.timeout(30 * 1000);

		var count = 10000;
		var autoconn = null;

		var startTick = null;
		var rate = null;
		return P.try(function(){
			return memdb.startServer(env.dbConfig('s1'));
		})
		.then(function(){
			autoconn = memdb.autoConnect();

			startTick = Date.now();
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				var promise = P.resolve();
				_.range(count).forEach(function(i){
					var doc = {_id : 1, name : 'rain', exp : i};
					promise = promise.then(function(){
						return Player.update(doc._id, doc, {upsert : true});
					});
				});
				return promise;
			});
		})
		.then(function(){
			rate = count * 1000 / (Date.now() - startTick);
		})
		.finally(function(){
			return memdb.stopServer()
			.finally(function(){
				logger.warn('Rate: %s', rate);
			});
		})
		.nodeify(cb);
	});

	it('write huge docs', function(cb){
		this.timeout(30 * 1000);

		var count = 10000;
		var autoconn = null;

		var startTick = null;
		var rate = null;
		return P.try(function(){
			var config = env.dbConfig('s1');
			//Disable auto persistent
			config.persistentInterval = 3600 * 1000;
			return memdb.startServer(config);
		})
		.then(function(){
			autoconn = memdb.autoConnect();

			startTick = Date.now();
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				var promise = P.resolve(); // jshint ignore:line
				_.range(count).forEach(function(id){
					var doc = {_id : id, name : 'rain', exp : id};
					promise = promise.then(function(){
						return Player.update(doc._id, doc, {upsert : true});
					});
				});
				return promise;
			});
		})
		.then(function(){
			rate = count * 1000 / (Date.now() - startTick);
			logger.warn('Load Rate: %s', rate);
			logger.warn('%j', process.memoryUsage());
			require('heapdump').writeSnapshot('/tmp/mdb.heapsnapshot');
		})
		.then(function(){
			startTick = Date.now();
			return memdb.stopServer();
		})
		.then(function(){
			rate = count * 1000 / (Date.now() - startTick);
			logger.warn('Unload Rate: %s', rate);
		})
		.nodeify(cb);
	});

	it('move huge docs across shards', function(cb){
		this.timeout(30 * 1000);

		var count = 1000, requestRate = 200;
		var db1 = null, db2 = null;
		var startTick = null, responseTimeTotal = 0;

		return P.try(function(){
			var config1 = env.dbConfig('s1');
			config1.persistentInterval = 3600 * 100;

			var config2 = env.dbConfig('s2');
			config2.persistentInterval = 3600 * 100;

			db1 = new Database(config1);
			db2 = new Database(config2);

			return P.all([db1.start(), db2.start()]);
		})
		.then(function(){
			var autoconn = new AutoConnection({db : db1});
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				var promise = P.resolve();
				_.range(count).forEach(function(id){
					var doc = {_id : id, name : 'rain', exp : id};
					promise = promise.then(function(){
						return Player.insert(doc._id, doc);
					});
				});
				return promise;
			});
		})
		.then(function(){
			startTick = Date.now();
			var autoconn = new AutoConnection({db : db2});

			return P.map(_.range(count), function(id){
				return P.delay(_.random(count * 1000 / requestRate))
				.then(function(){
					var start = null;
					return autoconn.execute(function(){
						start = Date.now();
						return autoconn.collection('player').remove(id);
					})
					.then(function(){
						responseTimeTotal += Date.now() - start;
					});
				});
			});
		})
		.then(function(){
			var rate = count * 1000 / (Date.now() - startTick);
			logger.warn('Rate: %s, Response Time: %s', rate, responseTimeTotal / count);
			return P.all([db1.stop(), db2.stop()]);
		})
		.nodeify(cb);
	});

	it('move one doc across shards', function(cb){
		this.timeout(180 * 1000);

		var moveSingleDoc = function(shardCount, requestRate, lockRetryInterval){
			if(!shardCount){
				shardCount = 8;
			}
			if(!requestRate){
				requestRate = 200;
			}
			if(!lockRetryInterval){
				lockRetryInterval = 100;
			}

			logger.warn('shardCount: %s, lockRetryInterval %s, requestRate: %s', shardCount, lockRetryInterval, requestRate);

			var requestCount = 1000;
			var shards = [];
			var startTick = null;
			var responseTimeTotal = 0;

			return P.try(function(){
				shards = _.range(1, shardCount + 1).map(function(shardId){
					var config = {
						_id : shardId,
						redis : env.config.redis,
						backend : env.config.backend,
						slave : env.config.redis,
						backendLockRetryInterval : lockRetryInterval,
					};
					return new Database(config);
				});

				return P.map(shards, function(shard){
					return shard.start();
				});
			})
			.then(function(){
				var conns = shards.map(function(shard){
					return new AutoConnection({db : shard});
				});

				var doc = {_id : 1, name : 'rain', exp : 0};

				var delay = 0;
				startTick = Date.now();

				return P.map(_.range(requestCount), function(requestId){
					return P.delay(_.random(requestCount * 1000 / requestRate))
					.then(function(){
						var conn = _.sample(conns);
						logger.info('start request %s on shard %s', requestId, conn.db.shard._id);

						var start = Date.now();
						return conn.execute(function(){
							var Player = conn.collection('player');
							return P.try(function(){
								return Player.findForUpdate(doc._id);
							})
							.then(function(ret){
								if(!ret){
									return Player.insert(doc._id, doc);
								}
								return Player.update(ret._id, {exp : ret.exp + 1});
							});
						})
						.catch(function(e){
							logger.warn(e);
						})
						.finally(function(){
							responseTimeTotal += Date.now() - start;
							logger.info('done request %s on shard %s', requestId, conn.db.shard._id);
						});
					});
				});
			})
			.then(function(){
				var rate = requestCount * 1000 / (Date.now() - startTick);
				logger.warn('Rate: %s, Response Time: %s', rate, responseTimeTotal / requestCount);
			})
			.then(function(){
				return P.map(shards, function(shard){
					return shard.stop();
				});
			});
		};

		/**
		 * According to perf test result, the recommended parameter and the system load is
		 *
		 * shards	retry	maxRate
		 * 4 		50 		450
		 * 8 		50 		400
		 * 16 		50 		350
		 * 32 		100		300
		 * 64 		100 	250
		 * 128 		200 	200
		 */
		var promise = P.resolve();
		var counts = [4, 8, 16, 32, 64, 128];
		var reqRate = 300;
		var retryDelay = 100;
		counts.forEach(function(count){
			promise = promise.then(function(){
				return moveSingleDoc(count, reqRate, retryDelay);
			});
		});
		promise.nodeify(cb);
	});
});

