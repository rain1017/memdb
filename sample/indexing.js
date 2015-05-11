'use strict';

// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var P = require('bluebird');

// memdb's config
var config = {
    //shard Id (Must unique and immutable for each shard)
    shard : 's1',
    // Center backend storage, must be same for all shards
    backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
    // Center redis used for backendLock, must be same for all shards
    redis : {host : '127.0.0.1', port : 6379},
    // Redis data replication (for current shard)
    slave : {host : '127.0.0.1', port : 6379, db : 1},

    // Config for each collections
    collections : {
        // Config for player collection
        player : {
            // Index config
            indexes : [
                // index for key [areaId]
                {
                    keys : ['areaId'],
                    // Values that exclude from index
                    // Since some default value occurs too often, which can make index too large
                    valueIgnore : {
                        areaId : ['', -1, null],
                    }
                },
                // Unique index for compound keys [deviceType, deviceId]
                {
                    keys : ['deviceType', 'deviceId'],
                    unique : true,
                }
            ]
        }
    }
};

var main = P.coroutine(function*(){
    // Start a memdb shard with in-process mode
    yield memdb.startServer(config);

    // Connect to memdb
    var autoconn = yield memdb.autoConnect();
    var Player = autoconn.collection('player');

    yield autoconn.transaction(P.coroutine(function*(){
        // Insert players
        var players = [{_id : 1, name : 'rain', areaId : 1},
                       {_id : 2, name : 'snow', areaId : 2}];
        yield Player.insert(players);
        // Find all players in area1
        console.log(yield Player.find({areaId : 1}));
        // Also ok, but return one doc rather than array of docs
        console.log(yield Player.find({_id : 1}));
        // DO NOT do this! Error will be thrown since name is not indexed.
        // yield Player.find({name : 'rain'});

        // Move all players in area1 into area2
        yield Player.update({areaId : 1}, {$set : {areaId : 2}});
        // Remove all players in area2
        yield Player.remove({areaId : 2});
    }));

    yield autoconn.transaction(P.coroutine(function*(){
        // Insert a player
        yield Player.insert({_id : 1, deviceType : 1, deviceId : 'id1'});

        // Find with compound key
        console.log(yield Player.find({deviceType : 1, deviceId : 'id1'}));

        // Will throw duplicate key error
        // yield Player.insert({deviceType : 1, deviceId : 'id1'});

        // Remove player
        yield Player.remove(1);
    }));

    // stop memdb server
    yield memdb.stopServer();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}

// For distributed system, just run memdb in each server with the same config, and each server will be a shard
