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
    beforeEach(env.flushdb);
    after(env.flushdb);

    it('standard', function(cb){
        this.timeout(3600 * 1000);

        var runTest = function(opts){
            opts = opts || {};

            var shardCount = opts.shardCount || 1;
            var playerCount = opts.playerCount || 100;
            var areaCount = Math.floor(opts.areaCount || (playerCount / 10));
            var transOps = opts.transOps || 5;
            var useIndex = opts.useIndex || false;
            var randomRoute = opts.randomRoute || false;

            var transCount = Math.floor(opts.transCount || (50000 / playerCount / transOps));
            if(transCount < 1){
                transCount = 1;
            }

            var shardIds = Object.keys(env.config.shards).slice(0, shardCount);

            var autoconn = null;

            var queryThread = function(threadId){
                var Dummy = useIndex ? autoconn.collection('player') : autoconn.collection('dummy');

                return P.each(_.range(transCount), function(){
                    var shardId = randomRoute ? _.sample(shardIds) : shardIds[threadId % shardIds.length];

                    // Make sure same areaId route to same shard
                    var areaId = randomRoute ? _.random(areaCount) : Math.floor(_.random(areaCount) / shardIds.length) * shardIds.length + threadId % shardIds.length;

                    var makeQuery = function(){
                        var modifier = useIndex ? {$set : {areaId : areaId}} : {$inc : {exp : 1}};
                        return Dummy.update(threadId, modifier, {upsert : true});
                    };

                    return autoconn.transaction(function(){
                        if(transOps === 1){
                            return makeQuery();
                        }
                        else{
                            return P.each(_.range(transOps), function(){
                                return makeQuery();
                            });
                        }
                    }, shardId)
                    .catch(function(e){
                        logger.error(e.stack);
                    });
                });
            };

            return env.startCluster(shardIds)
            .then(function(){
                var config = {
                    shards : env.config.shards,
                    maxPendingTask : 1000,
                };
                return memdb.autoConnect(config)
                .then(function(ret){
                    autoconn = ret;
                });
            })
            .then(function(){
                var startTick = Date.now();

                return P.map(_.range(playerCount), queryThread)
                .then(function(){
                    var baseRate = playerCount * 1000 / (Date.now() - startTick);
                    logger.warn(opts.description);
                    logger.warn('ShardCount: %s, PlayerCount: %s, ops/trans: %s', shardCount, playerCount, transOps);
                    logger.warn('ops: %s', baseRate * transOps * transCount);
                    logger.warn('tps: %s', baseRate * transCount);
                });
            })
            .then(function(){
                return autoconn.close();
            })
            .delay(1000)
            .finally(function(){
                return env.stopCluster();
            });
        };

        var testOpts = [
        {
            description : 'operations (1 shard)',
            shardCount : 1,
            playerCount : 100,
            transOps : 1000,
            transCount : 1,
        },
        {
            description : 'transactions(1op) (1 shard)',
            shardCount : 1,
            playerCount : 100,
            transOps : 1,
            transCount : 1000,
        },
        // {
        //     description : 'transactions(10ops) (1 shard)',
        //     shardCount : 1,
        //     playerCount : 100,
        //     transOps : 10,
        //     transCount : 100,
        // },
        // {
        //     description : 'operations with index (1 shard)',
        //     shardCount : 1,
        //     playerCount : 100,
        //     transOps : 1000,
        //     useIndex : true,
        //     transCount : 1,
        // },
        // {
        //     description : 'transactions(1op) with index (1 shard)',
        //     shardCount : 1,
        //     playerCount : 100,
        //     transOps : 1,
        //     transCount : 1000,
        //     useIndex : true,
        // },
        // {
        //     //bottle neck is client side
        //     description : 'transactions(1op) (2 shards)',
        //     shardCount : 2,
        //     playerCount : 100,
        //     transOps : 1,
        //     transCount : 1000,
        // },
        // {
        //     description : 'transactions(1op) (2 shards random route)',
        //     shardCount : 2,
        //     playerCount : 100,
        //     transOps : 1,
        //     transCount : 100,
        //     randomRoute : true,
        // },
        // {
        //     //bottle neck is client side
        //     description : 'operations (2 shards)',
        //     shardCount : 2,
        //     playerCount : 100,
        //     transOps : 1000,
        //     transCount : 1,
        // },
        // {
        //     description : 'transaction(1op) with index (2 shards)',
        //     shardCount : 2,
        //     playerCount : 100,
        //     transOps : 1,
        //     transCount : 1000,
        //     useIndex : true,
        // },
        ];

        return P.each(testOpts, function(testOpt){
            return P.try(function(){
                return P.promisify(env.flushdb);
            })
            .then(function(){
                return runTest(testOpt);
            })
            .then(function(){
                return P.promisify(env.flushdb);
            });
        })
        .nodeify(cb);
    });

    it('load/unload', function(cb){
        this.timeout(300 * 1000);

        var count = 20000;
        var concurrency = 100;
        var autoconn = null;

        return env.startCluster('s1', function(config){
            config.persistentDelay = 3600 * 1000;
            config.idleTimeout = 3600 * 1000;
        })
        .then(function(){
            return memdb.autoConnect(env.config)
            .then(function(ret){
                autoconn = ret;
            });
        })
        .then(function(){
            var startTick = Date.now();
            var Player = autoconn.collection('player');

            return P.map(_.range(concurrency), function(groupId){
                var groupCount = Math.floor(count/concurrency);

                return autoconn.transaction(function(){
                    return P.each(_.range(groupCount), function(index){
                        var doc = {_id : groupCount * groupId + index, name : 'rain'};
                        return Player.insert(doc);
                    });
                }, 's1');
            })
            .then(function(){
                var rate = count * 1000 / (Date.now() - startTick);
                logger.warn('Load Rate: %s', rate);
            });
        })
        .then(function(){
            var startTick = Date.now();
            var Player = autoconn.collection('player');

            return P.map(_.range(concurrency), function(groupId){
                var groupCount = Math.floor(count/concurrency);

                return autoconn.transaction(function(){
                    return P.each(_.range(groupCount), function(index){
                        return Player.update({_id : groupCount * groupId + index}, {$set : {name : 'snow'}}, {upsert : true});
                    });
                }, 's1');
            })
            .then(function(){
                var rate = count * 1000 / (Date.now() - startTick);
                logger.warn('Update Rate: %s', rate);
            });
        })
        .finally(function(){
            var startTick = Date.now();
            return env.stopCluster()
            .then(function(){
                var rate = count * 1000 / (Date.now() - startTick);
                logger.warn('Unload Rate: %s', rate);
            });
        })
        .nodeify(cb);
    });

    it('mdbgoose', function(cb){
        this.timeout(300 * 1000);

        var queryCount = 200, concurrency = 100;

        var mdbgoose = memdb.goose;
        delete mdbgoose.connection.models.player;

        var Player = mdbgoose.model('player', new mdbgoose.Schema({
            _id : String,
            exp : Number,
        }, {collection : 'player'}));

        return env.startCluster('s1')
        .then(function(){
            return mdbgoose.connectAsync(env.config);
        })
        .then(function(){
            var startTick = Date.now();

            return P.map(_.range(concurrency), function(playerId){
                return mdbgoose.transaction(function(){
                    var player = new Player({_id : playerId, exp : 0});

                    return P.each(_.range(queryCount), function(){
                        player.exp++;
                        return player.saveAsync();
                    });
                }, 's1');
            })
            .then(function(){
                var rate = queryCount * concurrency * 1000 / (Date.now() - startTick);
                logger.warn('Rate: %s', rate);
            });
        })
        .then(function(){
            return mdbgoose.disconnectAsync();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });


    it.skip('gc & idle', function(cb){
        this.timeout(3600 * 1000);

        return env.startCluster('s1', function(config){
            config.memoryLimit = 1024; // 1G

            // Set large value to trigger gc, small value to not trigger gc
            config.idleTimeout = 3600 * 1000;
        })
        .then(function(){
            return memdb.autoConnect(env.config);
        })
        .then(function(autoconn){
            return P.each(_.range(200000), function(i){
                var doc = {_id : i};
                for(var j=0; j<10; j++){
                    doc['key' + j] = 'value' + j;
                }
                return autoconn.transaction(function(){
                    return autoconn.collection('player').insert(doc);
                }, 's1');
            });
        })
        .then(function(){
            logger.warn('%j', process.memoryUsage());
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });
});

