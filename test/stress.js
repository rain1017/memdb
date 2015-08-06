// Copyright 2015 The MemDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

'use strict';

// run with node >= 0.12 with --harmony option

// If logLevel >= INFO, change log4js.json - set appender type to fileSync (otherwise log buffer may eat out memory)

var minimist = require('minimist');
var crypto = require('crypto');
var P = require('bluebird');
var _ = require('lodash');
var uuid = require('node-uuid');
var env = require('./env');
var memdb = require('../lib');
var mongodb = P.promisifyAll(require('mongodb'));
var logger = memdb.logger.getLogger('test', __filename);

var isMaster = true;
var concurrency = 1000;
var areaCount = 200;
var maxAreaPlayers = 10;
var randomRoute = false; // turn on this can slow down performance

var newPlayerIntervalValue = 10;
var currentConcurrency = 0;

var route = function(id){
    var shardIds = Object.keys(env.config.shards);
    if(id === null || id === undefined){
        return _.sample(shardIds);
    }

    var md5 = crypto.createHash('md5').update(String(id)).digest('hex');
    var hash = parseInt(md5.substr(0, 8), 16);

    var index = randomRoute ? _.random(shardIds.length - 1) : (hash % shardIds.length);
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
            if(area.playerCount >= maxAreaPlayers){
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
            yield P.delay(_.random(1000));

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
        yield P.delay(_.random(10 * 1000));

        var areaId = _.random(areaCount);
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
        clearInterval(newPlayerInterval);
        if(isMaster){
            env.stopCluster();
            yield checkConsistency();
        }
    }
    catch(e){
        logger.error(e.stack);
    }
    finally{
        memdb.logger.shutdown(process.exit);
    }
});

var main = P.coroutine(function*(opts){
    isMaster = !opts.slave;
    if(opts.hasOwnProperty('concurrency')){
        concurrency = parseInt(opts.concurrency);
    }
    if(opts.hasOwnProperty('areaCount')){
        areaCount = parseInt(opts.areaCount);
    }
    if(opts.hasOwnProperty('maxAreaPlayers')){
        maxAreaPlayers = parseInt(opts.maxAreaPlayers);
    }
    if(opts.hasOwnProperty('randomRoute')){
        randomRoute = !!opts.randomRoute;
    }

    if(isMaster){
        env.flushdb();
        env.startCluster();
    }

    var autoconn = yield memdb.autoConnect(env.config);

    newPlayerInterval = setInterval(P.coroutine(function*(){
        if(currentConcurrency >= concurrency){
           return;
        }

        currentConcurrency++;

        var playerId = uuid.v4();
        try{
            logger.warn('player %s start', playerId);
            logger.warn('current concurrency: %s', currentConcurrency);
            yield playerThread(autoconn, playerId);
        }
        catch(e){
            logger.error('player %s error: %s', playerId, e.stack);
        }
        finally{
            logger.warn('player %s finish', playerId);
            currentConcurrency--;
        }
    }), newPlayerIntervalValue);

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
});

if (require.main === module) {
    var opts = minimist(process.argv.slice(2));
    main(opts);
}
