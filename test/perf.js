'use strict';

var P = require('bluebird');
var _ = require('lodash');
var utils = require('../app/utils');
var should = require('should');
var env = require('./env');
var memdb = require('../lib');
var Database = require('../app/database');
var AutoConnection = require('../lib/autoconnection');
var logger = require('memdb-logger').getLogger('test', __filename);

describe.skip('performance test', function(){
    beforeEach(function(cb){
        env.flushdb(cb);
    });
    after(function(cb){
        env.flushdb(cb);
    });

    it('single doc transaction', function(cb){
        this.timeout(30 * 1000);

        var count = 10000;
        var autoconn = null;
        var player = {_id : 1, name : 'rain', exp : 0};

        var incPlayerExp = function(){
            var Player = autoconn.collection('player');
            return autoconn.transaction(function(){
                return Player.update(player._id, {$inc : {exp : 1}});
            });
        };

        var startTick = null;
        var rate = null;
        return P.try(function(){
            return memdb.startServer(env.dbConfig('s1'));
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(ret){
            autoconn = ret;
            return autoconn.transaction(function(){
                var Player = autoconn.collection('player');
                return Player.insert(player);
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

            return autoconn.transaction(function(){
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

    it('single doc in transaction query', function(cb){
        this.timeout(30 * 1000);

        var count = 50000;
        var autoconn = null;

        var startTick = null;
        var rate = null;
        return P.try(function(){
            return memdb.startServer(env.dbConfig('s1'));
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(ret){
            autoconn = ret;

            startTick = Date.now();
            return autoconn.transaction(function(){
                var Player = autoconn.collection('player');

                return P.reduce(_.range(count), function(sofar, value){
                    return Player.update(1, {$inc : {exp : 1}}, {upsert : true});
                }, 0);
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

    it('single doc in transation query with update index', function(cb){
        this.timeout(300 * 1000);

        var count = 20000;
        var autoconn = null;

        return P.try(function(){
            return memdb.startServer(env.dbConfig('s1'));
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(ret){
            autoconn = ret;

            var Player = autoconn.collection('player');
            var startTick = Date.now();

            return autoconn.transaction(function(){
                var p = P.resolve();
                _.range(count).forEach(function(){
                    p = p.then(function(){
                        return Player.update({_id : 1}, {$set : {areaId : _.random(100)}}, {upsert: true});
                    });
                });
                return p;
            })
            .then(function(){
                var rate = count * 1000 / (Date.now() - startTick);
                logger.warn('Rate: %s', rate);
            });
        })
        .then(function(){
            return autoconn.close();
        })
        .finally(function(){
            return memdb.stopServer();
        })
        .nodeify(cb);
    });

    it('multiple docs transaction', function(cb){
        this.timeout(30 * 1000);

        var count = 10000;
        var autoconn = null;

        var startTick = null;
        var rate = null;
        return P.try(function(){
            var config = env.dbConfig('s1');
            //Disable auto persistent
            config.persistentDelay = 3600 * 1000;
            config.idleTimeout = 3600 * 1000;
            return memdb.startServer(config);
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(ret){
            autoconn = ret;

            startTick = Date.now();
            return autoconn.transaction(function(){
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

    it('multiple docs fly across shards', function(cb){
        this.timeout(30 * 1000);

        var count = 1000, requestRate = 300;
        var db1 = null, db2 = null;
        var startTick = null, responseTimeTotal = 0;

        return P.try(function(){
            var config1 = env.dbConfig('s1');
            config1.persistentDelay = 3600 * 100;

            var config2 = env.dbConfig('s2');
            config2.persistentDelay = 3600 * 100;

            db1 = new Database(config1);
            db2 = new Database(config2);

            return P.all([db1.start(), db2.start()]);
        })
        .then(function(){
            var autoconn = new AutoConnection({db : db1});
            return autoconn.transaction(function(){
                var Player = autoconn.collection('player');
                var promise = P.resolve();
                _.range(count).forEach(function(id){
                    var doc = {_id : id, name : 'rain', exp : id};
                    promise = promise.then(function(){
                        return Player.insert(doc);
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
                    return autoconn.transaction(function(){
                        start = Date.now();
                        return autoconn.collection('player').remove(id);
                    })
                    .then(function(){
                        responseTimeTotal += Date.now() - start;
                    });
                })
                .catch(function(e){
                    logger.warn(e);
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

    it('one doc accessed by random shards', function(cb){
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
                        shardId : shardId,
                        locking : env.config.shards.s1.locking,
                        event : env.config.shards.s1.event,
                        backend : env.config.shards.s1.backend,
                        slave : env.config.shards.s1.slave,
                        persistentDelay : 3600 * 1000,
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

                var delay = 0;
                startTick = Date.now();

                return P.map(_.range(requestCount), function(requestId){
                    return P.delay(_.random(requestCount * 1000 / requestRate))
                    .then(function(){
                        var conn = _.sample(conns);
                        logger.info('start request %s on shard %s', requestId, conn.db.shard._id);

                        var start = Date.now();
                        return conn.transaction(function(){
                            var Player = conn.collection('player');
                            return Player.update(1, {$inc : {exp : 1}}, {upsert : true});
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
         * shards   retry   maxRate
         * 4        50      450
         * 8        50      400
         * 16       50      350
         * 32       100     300
         * 64       100     250
         * 128      200     200
         */
        var promise = P.resolve();
        var counts = [4, 8, 16, 32, 64, 128];
        var reqRate = 500;
        var retryDelay = 100;
        counts.forEach(function(count){
            promise = promise.then(function(){
                return moveSingleDoc(count, reqRate, retryDelay);
            });
        });
        promise.nodeify(cb);
    });

    it('mdbgoose single doc in transation query', function(cb){
        this.timeout(300 * 1000);

        var count = 10000;

        var mdbgoose = memdb.goose;
        delete mdbgoose.connection.models.player;

        var Player = mdbgoose.model('player', new mdbgoose.Schema({
            _id : String,
            exp : Number,
        }, {collection : 'player'}));

        return P.try(function(){
            var config = env.dbConfig('s1');
            config.collections = mdbgoose.genCollectionConfig();
            return memdb.startServer(config);
        })
        .then(function(){
            return mdbgoose.connectAsync();
        })
        .then(function(){
            var startTick = Date.now();

            return mdbgoose.transaction(function(){
                var p = P.resolve();
                var player = new Player({_id : 1, exp : 0});
                _.range(count).forEach(function(){
                    p = p.then(function(){
                        player.exp++;
                        return player.saveAsync();
                    });
                });
                return p;
            })
            .then(function(){
                var rate = count * 1000 / (Date.now() - startTick);
                logger.warn('Rate: %s', rate);
            });
        })
        .then(function(){
            return mdbgoose.disconnectAsync();
        })
        .finally(function(){
            return memdb.stopServer();
        })
        .nodeify(cb);
    });

    it('mdbgoose multi doc transaction and index', function(cb){
        this.timeout(3600 * 1000);

        var count = 5000;

        var mdbgoose = memdb.goose;
        delete mdbgoose.connection.models.player;

        var Player = mdbgoose.model('player', new mdbgoose.Schema({
            _id : String,
            areaId : {type : String, index : true},
            exp : Number,
        }, {collection : 'player'}));

        var makeTransaction = function(){
            return mdbgoose.transaction(function(){
                var playerId = _.random(100);
                var areaId = _.random(10);
                return P.try(function(){
                    return Player.findAsync(playerId);
                })
                .then(function(player){
                    if(!player){
                        player = new Player({_id : playerId, exp : 0});
                    }
                    player.exp++;
                    player.areaId = areaId;
                    return player.saveAsync();
                });
            });
        };

        return P.try(function(){
            var config = env.dbConfig('s1');
            config.collections = mdbgoose.genCollectionConfig();
            return memdb.startServer(config);
        })
        .then(function(){
            return mdbgoose.connectAsync();
        })
        .then(function(){
            var startTick = Date.now();

            var p = P.resolve();
            for(var i=0; i<count; i++){
                p = p.then(makeTransaction);
            }
            return p.then(function(){
                var rate = count * 1000 / (Date.now() - startTick);
                logger.warn('Rate: %s', rate);
            });
        })
        .then(function(){
            return mdbgoose.disconnectAsync();
        })
        .finally(function(){
            return memdb.stopServer();
        })
        .nodeify(cb);
    });

    it('gc & idle', function(cb){
        this.timeout(3600 * 1000);

        var config = env.dbConfig('s1');
        config.memoryLimit = 1024; //1G

        // Set large value to trigger gc, small value to not trigger gc
        config.idleTimeout = 3600 * 1000;

        return P.try(function(){
            return memdb.startServer(config);
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(autoconn){
            var p = P.resolve();
            _.range(10000).forEach(function(i){
                p = p.then(function(){
                    var doc = {_id : i};
                    for(var j=0; j<1000; j++){
                        doc['key' + j] = 'value' + j;
                    }
                    logger.warn('%s %j', i, process.memoryUsage());
                    return autoconn.transaction(function(){
                        return autoconn.collection('player').insert(doc);
                    });
                });
            });
            return p;
        })
        .then(function(){
            logger.warn('%j', process.memoryUsage());
        })
        .finally(function(){
            return memdb.stopServer();
        })
        .nodeify(cb);
    });
});

