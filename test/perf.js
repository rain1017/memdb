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

    it('multiple shards', function(cb){
        this.timeout(3600 * 1000);

        var runTest = function(opts){
            opts = opts || {};

            var shardCount = opts.shardCount || 1;
            var playerCount = opts.playerCount || 100;
            var areaCount = Math.floor(opts.areaCount || (playerCount / 10));
            var queryPerTrans = opts.queryPerTrans || 5;
            var useIndex = opts.useIndex || false;
            var randomRoute = opts.randomRoute || false;

            var transCount = Math.floor(opts.transCount || (50000 / playerCount / queryPerTrans));
            if(transCount < 1){
                transCount = 1;
            }

            var shardIds = Object.keys(env.config.shards).slice(0, shardCount);

            var autoconn = null;

            var queryThread = function(threadId){
                var Player = autoconn.collection('player');

                return P.each(_.range(transCount), function(){
                    var shardId = randomRoute ? _.sample(shardIds) : shardIds[threadId % shardIds.length];

                    // Make sure same areaId route to same shard
                    var areaId = randomRoute ? _.random(areaCount) : Math.floor(_.random(areaCount) / shardIds.length) * shardIds.length + threadId % shardIds.length;

                    return autoconn.transaction(function(){
                        return P.each(_.range(queryPerTrans), function(){
                            var modifier = null;
                            if(useIndex){
                                modifier = {$set : {areaId : areaId}};
                            }
                            else{
                                modifier = {$set : {level : _.random(100)}};
                            }

                            return Player.update(threadId, modifier, {upsert : true});
                        });
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
                    logger.warn('ShardCount: %s, PlayerCount: %s, QueryPerTrans: %s', shardCount, playerCount, queryPerTrans);
                    logger.warn('Transaction rate: %s', baseRate * transCount);
                    logger.warn('Query rate: %s', baseRate * queryPerTrans * transCount);
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
            description : 'Transaction 1 shard',
            shardCount : 1,
            playerCount : 200,
            queryPerTrans : 1,
            transCount : 500,
        },
        {
            description : 'Query 1 shard',
            shardCount : 1,
            playerCount : 200,
            queryPerTrans : 1000,
            transCount : 1,
        },
        // {
        //     //bottle neck is client side
        //     description : 'Transaction 2 shards',
        //     shardCount : 2,
        //     playerCount : 200,
        //     queryPerTrans : 1,
        //     transCount : 500,
        // },
        // {
        //     //bottle neck is client side
        //     description : 'Query 2 shards',
        //     shardCount : 2,
        //     playerCount : 200,
        //     queryPerTrans : 1000,
        //     transCount : 1,
        // },
        // {
        //     description : 'Transaction 2 shards random route',
        //     shardCount : 2,
        //     playerCount : 200,
        //     queryPerTrans : 1,
        //     transCount : 100,
        //     randomRoute : true,
        // },
        // {
        //     description : 'Indexed transaction 1 shard',
        //     shardCount : 1,
        //     playerCount : 200,
        //     queryPerTrans : 1,
        //     transCount : 500,
        //     useIndex : true,
        // },
        // {
        //     description : 'Indexed query 1 shard',
        //     shardCount : 1,
        //     playerCount : 200,
        //     queryPerTrans : 1000,
        //     useIndex : true,
        //     transCount : 1,
        // },
        // {
        //     description : 'Indexed transaction 2 shards',
        //     shardCount : 2,
        //     playerCount : 200,
        //     queryPerTrans : 1,
        //     transCount : 500,
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


    it('gc & idle', function(cb){
        this.timeout(3600 * 1000);

        return env.startCluster('s1', function(config){
            config.memoryLimit = 1024; // 1G

            // Set large value to trigger gc, small value to not trigger gc
            config.idleTimeout = 3600 * 1000;
        })
        .then(function(){
            return memdb.connectAsync(env.config);
        })
        .then(function(autoconn){
            return P.each(_.range(10000), function(i){
                var doc = {_id : i};
                for(var j=0; j<1000; j++){
                    doc['key' + j] = 'value' + j;
                }
                logger.warn('%s %j', i, process.memoryUsage());
                return autoconn.transaction(function(){
                    return autoconn.collection('player').insert(doc);
                });
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

