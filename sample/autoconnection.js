'use strict';

// npm install memdb-client
// run with node >= 0.12 with --harmony option

// first start memdb shards 's1' on localhost:31017, 's2' on localhost:31018.

var memdb = require('memdb-client');
// just bluebird promise
var P = memdb.Promise;

var main = P.coroutine(function*(){
    // All database access should via this autoconn object, you can preserve autoconn object in a global module that can be accessed anywhere
    var autoconn = yield memdb.autoConnect({
        shards : { // Specify all shards here
            s1 : {host : '127.0.0.1', port : 31017},
            s2 : {host : '127.0.0.1', port : 31018},
        }
    });

    var doc = {_id : '1', name : 'rain', level : 1};

    // Get player collection object
    var Player = autoconn.collection('player');

    // Make a transaction in shard s1
    yield autoconn.transaction(P.coroutine(function*(){
        // Upsert a doc (update if exist, insert if not exist)
        yield Player.update(doc._id, doc, {upsert : true});
        // Find the doc
        var ret = yield Player.find(doc._id);
        console.log(ret); // {_id : '1', name : 'rain', level : 1}
    }), 's1'); // Auto commit after transaction

    try{
        // Make another transaction in shard s1
        yield autoconn.transaction(P.coroutine(function*(){
            // Update doc with $set modifier
            yield Player.update(doc._id, {$set : {level : 2}});
            // Find the changed doc with specified field
            var ret = yield Player.find(doc._id, 'level');
            console.log(ret); // {level : 2}
            // Exception here!
            throw new Error('Oops!');
        }), 's1');
    }
    catch(err){ // Catch the exception
        // Change is rolled back
        yield autoconn.transaction(P.coroutine(function*(){
            var ret = yield Player.find(doc._id, 'level');
            console.log(ret); // {level : 1}
        }), 's1');
    }

    // Make transcation in another shard
    // Since we just accessed this doc in s1, the doc will 'fly' from shard s1 to s2
    // In real production you should avoid these kind of data 'fly' by routing transaction to proper shard
    yield autoconn.transaction(P.coroutine(function*(){
        yield Player.remove(doc._id);
    }), 's2');

    // Close all connections
    yield autoconn.close();
});

if (require.main === module) {
    main().finally(process.exit);
}
