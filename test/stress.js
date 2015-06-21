'use strict';

// run with node >= 0.12 with --harmony option

// If logLevel >= INFO, change log4js.json - set appender type to fileSync (otherwise log buffer may eat out memory)

var P = require('bluebird');
var _ = require('lodash');
var env = require('./env');
var memdb = require('../lib');
var mongodb = P.promisify(require('mongodb'));
var logger = memdb.logger.getLogger('test', __filename);

var maxConcurrency = 200;
var areaPlayerCount = 10;
var newPlayerIntervalValue = 10;

var maxPlayerId = 0;
var concurrency = 0;

var route = function(id){
    var shardIds = Object.keys(env.config.shards);
    if(id === null || id === undefined){
        return _.sample(shardIds);
    }
    var index = parseInt(id) % shardIds.length;

    //var index = Math.floor((parseInt(id) % 4) / 2);
    //logger.warn(shardIds[index]);
    return shardIds[index];
};

var playerThread = P.coroutine(function*(db, playerId){

    var Player = db.collection('player');
    var Area = db.collection('area');

    var playInArea = P.coroutine(function*(playerId, areaId){
        // join area
        var success = yield db.transaction(P.coroutine(function*(){

            var area = yield Area.find(areaId);
            if(!area){
                area = {_id : areaId, playerCount : 0};
                yield Area.insert(area);
            }
            if(area.playerCount >= areaPlayerCount){
                return false;
            }
            yield Area.update(areaId, {$inc : {playerCount : 1}});
            yield Player.update(playerId, {$set : {areaId : areaId}});
            return true;

        }), route(areaId));

        if(!success){
            return;
        }

        for(var i=0; i<_.random(10); i++){
            yield P.delay(_.random(100));

            // earn scores from other players in the area
            yield db.transaction(P.coroutine(function*(){
                var players = yield Player.find({areaId : areaId});
                if(players.length <= 1){
                    return;
                }

                for(var i=0; i<players.length; i++){
                    var player = players[i];
                    if(player._id !== playerId){
                        yield Player.update(player._id, {$set : {score : player.score - 1}});
                    }
                    else{
                        yield Player.update(player._id, {$set : {score : player.score + players.length - 1}});
                    }
                }
            }), route(areaId));
        }

        // quit area
        yield db.transaction(P.coroutine(function*(){

            yield Area.update(areaId, {$inc : {playerCount : -1}});

            yield Player.update(playerId, {$set : {areaId : null}});

            var area = yield Area.find(areaId);
            if(area.playerCount === 0){
                yield Area.remove(areaId);
            }

        }), route(areaId));
    });

    yield db.transaction(P.coroutine(function*(){
        yield Player.insert({_id : playerId, areaId : null, score : 0});
    }), route(playerId));

    for(var i=0; i<_.random(10); i++){
        yield P.delay(_.random(1000));

        var areaId = _.random(Math.floor(maxConcurrency / areaPlayerCount) + 1);
        yield playInArea(playerId, areaId);
    }

    // yield db.transaction(P.coroutine(function*(){
    //     yield Player.remove(playerId);
    // }), route(playerId));
});

var checkConsistency = P.coroutine(function*(){
    var db = yield P.promisify(mongodb.MongoClient.connect)(env.config.backend.url);

    var playerCount = yield db.collection('player').countAsync();
    var areaCount = yield db.collection('area').countAsync();
    logger.warn('playerCount: %s, areaCount: %s', playerCount, areaCount);

    var ret = yield db.collection('player').aggregateAsync([{$group : {_id : null, total : {$sum :'$score'}}}]);

    if(ret[0].total !== 0){
        logger.fatal('consistency check FAIL');
    }
    else{
        logger.warn('consistency check PASS');
    }
});


var isShutingDown = false;
var newPlayerInterval = null;

var shutdown = P.coroutine(function*(){
    if(isShutingDown){
        return;
    }
    isShutingDown = true;

    try{
        clearTimeout(newPlayerInterval);
        yield env.stopCluster();
        yield checkConsistency();
    }
    catch(e){
        logger.error(e.stack);
    }
    finally{
        setTimeout(process.exit, 100);
    }
});

var main = P.coroutine(function*(){
    yield env.flushdb();
    yield env.startCluster();

    var autoconn = yield memdb.autoConnect(env.config);

    newPlayerInterval = setInterval(P.coroutine(function*(){
        if(concurrency >= maxConcurrency){
            return;
        }

        concurrency++;
        maxPlayerId++;

        var playerId = maxPlayerId.toString();
        try{
            logger.warn('player %s start', playerId);
            logger.warn('current concurrency: %s', concurrency);
            yield playerThread(autoconn, playerId);
        }
        catch(e){
            logger.error('player %s error: %s', playerId, e.stack);
        }
        finally{
            logger.warn('player %s finish', playerId);
            concurrency--;
        }
    }), newPlayerIntervalValue);

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
});

if (require.main === module) {
    main();
}
