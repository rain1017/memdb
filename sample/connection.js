'use strict';

// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var P = require('bluebird');

// memdb's config
var config = {
    //shard Id (Must unique and immutable for each shard)
    shardId : 's1',
    // Global backend storage, all shards must connect to the same mongodb (or mongodb cluster)
    backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
    // Global locking redis, all shards must connect to the same redis (or redis cluster)
    locking : {host : '127.0.0.1', port : 6379, db : 0},
    // Global event redis, all shards must connect to the same redis
    event : {host : '127.0.0.1', port : 6379, db : 0},
    // Data replication redis, one redis instance for each shard
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

// Start more shards that connect to __the same global services (backend/locking/event)__
// They will automatically become a MemDB cluster.
