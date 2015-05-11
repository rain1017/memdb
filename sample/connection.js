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
};

var main = P.coroutine(function*(){
    // Start a memdb shard with in-process mode
    yield memdb.startServer(config);

    // Create a new connection
    var conn = yield memdb.connect();
    // Get player collection
    var Player = conn.collection('player');
    // Insert a doc
    var player = {_id : 'p1', name : 'rain', level : 1};
    yield Player.insert(player);
    // Commit changes
    yield conn.commit();
    // Update a field
    yield Player.update(player._id, {$set : {level : 2}});
    // Find the doc (only return specified field)
    console.log(yield Player.find(player._id, 'level')); // should print {level : 2}
    // Rollback changes
    yield conn.rollback();
    // Data restore to last commited state
    console.log(yield Player.find(player._id, 'level')); // should print {level : 1}
    // Remove doc
    yield Player.remove(player._id);
    // Commit change
    yield conn.commit();
    // close connection
    yield conn.close();

    // stop memdb server
    yield memdb.stopServer();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}

// For distributed system, just run memdb in each server with the same config, and each server will be a shard
