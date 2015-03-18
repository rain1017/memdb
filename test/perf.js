'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var env = require('./env');
var memorydb = require('../lib');
var Database = require('../lib/database');
var AutoConnection = require('../lib/client/autoconnection');

var pomeloLogger = require('pomelo-logger');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe.skip('performance test', function(){
	beforeEach(function(cb){
		Q.longStackSupport = false;
		pomeloLogger.setGlobalLogLevel(pomeloLogger.levels.WARN);

		env.flushdb(cb);
	});
	after(function(cb){
		Q.longStackSupport = true;
		pomeloLogger.setGlobalLogLevel(pomeloLogger.levels.ALL);

		env.flushdb(cb);
	});

	it('write single doc', function(cb){
		this.timeout(30 * 1000);

		var count = 1000;
		var autoconn = null;
		var player = {_id : 1, name : 'rain', exp : 0};

		var incPlayerExp = function(){
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				return Q.fcall(function(){
					// player.exp = _.random(60);
					// return Player.update(player._id, player);
					return Player.findForUpdate(player._id);
				})
				.then(function(doc){
					doc.exp++;
					logger.debug('player.exp = %s', doc.exp);
					return Player.update(player._id, doc);
				});
			})
			.catch(function(e){
				logger.error(e);
			});
		};

		var startTick = null;
		var rate = null;
		return Q.fcall(function(){
			var config = env.dbConfig('s1');
			//config.disableSlave = true;
			return memorydb.start(config);
		})
		.then(function(){
			autoconn = memorydb.autoConnect();
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				return Player.insert(player._id, player);
			});
		})
		.then(function(){
			startTick = Date.now();
			var promise = Q(); // jshint ignore:line
			for(var i=0; i<count; i++){
				promise = promise.then(incPlayerExp);
			}
			return promise;
		})
		.then(function(){
			rate = count * 1000 / (Date.now() - startTick);
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				return Q.fcall(function(){
					return Player.find(player._id);
				})
				.then(function(ret){
					logger.warn('%j', ret);

					return Player.remove(player._id);
				});
			});
		})
		.fin(function(){
			return memorydb.stop()
			.fin(function(){
				logger.warn('Rate: %s', rate);
			});
		})
		.nodeify(cb);
	});

	it('write single doc in one transcation', function(cb){
		this.timeout(30 * 1000);

		var count = 10000;
		var autoconn = null;

		var startTick = null;
		var rate = null;
		return Q.fcall(function(){
			return memorydb.start(env.dbConfig('s1'));
		})
		.then(function(){
			autoconn = memorydb.autoConnect();

			startTick = Date.now();
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				var promise = Q(); // jshint ignore:line
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
		.fin(function(){
			return memorydb.stop()
			.fin(function(){
				logger.warn('Rate: %s', rate);
			});
		})
		.nodeify(cb);
	});

	it('write huge docs', function(cb){
		this.timeout(30 * 1000);

		//TODO: Unload rate decreased when count >= 5000
		var count = 1000;
		var autoconn = null;

		var startTick = null;
		var rate = null;
		return Q.fcall(function(){
			var config = env.dbConfig('s1');
			//Disable auto persistent
			config.persistentInterval = 3600 * 1000;
			return memorydb.start(config);
		})
		.then(function(){
			autoconn = memorydb.autoConnect();

			startTick = Date.now();
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				var promise = Q(); // jshint ignore:line
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
			//heapdump.writeSnapshot('/tmp/mdb.heapsnapshot');
		})
		.then(function(){
			startTick = Date.now();
			return memorydb.stop();
		})
		.then(function(){
			rate = count * 1000 / (Date.now() - startTick);
			logger.warn('Unload Rate: %s', rate);
		})
		.nodeify(cb);
	});

	it('move huge docs across shards', function(cb){
		this.timeout(30 * 1000);

		var count = 1000;
		var db1 = null, db2 = null;
		var startTick = null;

		return Q.fcall(function(){
			var config1 = env.dbConfig('s1');
			config1.persistentInterval = 3600 * 100;

			var config2 = env.dbConfig('s2');
			config2.persistentInterval = 3600 * 100;

			db1 = new Database(config1);
			db2 = new Database(config2);

			return Q.all([db1.start(), db2.start()]);
		})
		.then(function(){
			var autoconn = new AutoConnection(db1);
			return autoconn.execute(function(){
				var Player = autoconn.collection('player');
				var promise = Q(); // jshint ignore:line
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
			var autoconn = new AutoConnection(db2);

			return Q.all(_.range(count).map(function(id){
				return autoconn.execute(function(){
					return autoconn.collection('player').remove(id);
				});
			}));
		})
		.then(function(){
			var rate = count * 1000 / (Date.now() - startTick);
			logger.warn('Rate: %s', rate);
			return Q.all([db1.stop(), db2.stop()]);
		})
		.nodeify(cb);
	});

	it('move one doc across shards', function(cb){
		this.timeout(180 * 1000);

		var moveSingleDoc = function(shardCount, lockRetryInterval, concurrency){
			if(!shardCount){
				shardCount = 8;
			}
			if(!lockRetryInterval){
				lockRetryInterval = 100;
			}
			if(!concurrency){
				concurrency = 128;
			}

			var requestCount = concurrency * 10;

			logger.warn('shardCount: %s, lockRetryInterval %s, requestCount: %s, concurrency: %s', shardCount, lockRetryInterval, requestCount, concurrency);

			var shards = [];
			var startTick = null;
			var responseTimeTotal = 0;

			return Q.fcall(function(){
				shards = _.range(1, shardCount + 1).map(function(shardId){
					var config = env.dbConfig(shardId);
					config.backendLockRetryInterval = lockRetryInterval;

					return new Database(config);
				});

				return Q.all(shards.map(function(shard){
					return shard.start();
				}));
			})
			.then(function(){
				var conns = shards.map(function(shard){
					return new AutoConnection(shard);
				});

				var doc = {_id : 1, name : 'rain', exp : 0};

				var delay = 0;
				startTick = Date.now();

				return Q.all(_.range(concurrency).map(function(threadId){
					var promise = Q(); //jshint ignore:line
					_.range(requestCount / concurrency).forEach(function(requestId){
						promise = promise.then(function(){
							var conn = _.sample(conns);
							logger.info('start request %s:%s on shard %s', threadId, requestId, conn.db.shard._id);

							var start = Date.now();
							return conn.execute(function(){
								var Player = conn.collection('player');
								return Q.fcall(function(){
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
								logger.error(e);
							})
							.fin(function(){
								responseTimeTotal += Date.now() - start;
								logger.info('done request %s:%s on shard %s', threadId, requestId, conn.db.shard._id);
							});
						});
					});
					return promise;
				}));
			})
			.then(function(){
				var rate = requestCount * 1000 / (Date.now() - startTick);
				logger.warn('Rate: %s, Response Time: %s', rate, responseTimeTotal / requestCount);
			})
			.then(function(){
				return Q.all(shards.map(function(shard){
					return shard.stop();
				}));
			});
		};

		var promise = Q(); //jshint ignore:line
		var counts = [4, 8, 16, 32, 64, 128];
		counts.forEach(function(count){
			promise = promise.then(function(){
				return moveSingleDoc(count);
			});
		});
		promise.nodeify(cb);
	});
});
