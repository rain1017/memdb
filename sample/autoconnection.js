'use strict';

// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

// First start memdb server by:
// memdbd --conf=test/memdb.json --shard=s1
// memdbd --conf=test/memdb.json --shard=s2

var memdb = require('memdb');
var P = require('bluebird');
var should = require('should');

var main = P.coroutine(function*(){

    var autoconn = yield memdb.autoConnect({
        shards : {
            s1 : {host : '127.0.0.1', port : 31017},
            s2 : {host : '127.0.0.1', port : 31018},
        }
    });

    var User = autoconn.collection('user');
    var doc = {_id : '1', name : 'rain', level : 1};

    // Make a transaction in shard s1
    yield autoconn.transaction(P.coroutine(function*(){
        // Remove if exist
        yield User.remove(doc._id);

        // Insert a doc
        yield User.insert(doc);
        // Find the doc
        var ret = yield User.find(doc._id);
        ret.level.should.eql(1);
    }), 's1'); // Auto commit after transaction

    try{
        // Make another transaction in shard s1
        yield autoconn.transaction(P.coroutine(function*(){
            // Update doc with $set modifier
            yield User.update(doc._id, {$set : {level : 2}});
            // Find the changed doc
            var ret = yield User.find(doc._id);
            ret.level.should.eql(2);
            // Exception here!
            throw new Error('Oops!');
        }), 's1');
    }
    catch(err){ // Catch the exception
        // Change is rolled back
        yield autoconn.transaction(P.coroutine(function*(){
            var ret = yield User.find(doc._id);
            ret.level.should.eql(1);
        }), 's1');
    }

    // Make transcation in another shard
    yield autoconn.transaction(P.coroutine(function*(){
        yield User.remove(doc._id);
    }), 's2');

    yield autoconn.close();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}
