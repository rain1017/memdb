'use strict';

// npm install memdb-client
// run with node >= 0.12 with --harmony option

// We assure you have started shard 's1' on localhost:31017

var memdb = require('memdb-client');
var P = memdb.Promise;
var should = require('should');

var main = P.coroutine(function*(){
    // Create a new connection
    var conn = yield memdb.connect({host : '127.0.0.1', port : 31017});

    // Get player collection
    var Player = conn.collection('player');

    var player = {_id : 'p1', name : 'rain', level : 1};
    // remove if exist
    yield Player.remove(player._id);
    // Insert a doc
    yield Player.insert(player);
    // Commit changes
    yield conn.commit();
    // Update a field
    yield Player.update(player._id, {$set : {level : 2}});
    // Find the doc (only return specified field)
    (yield Player.find(player._id, 'level')).should.eql({level : 2});
    // Rollback changes
    yield conn.rollback();
    // Data restore to last commited state
    (yield Player.find(player._id, 'level')).should.eql({level : 1});
    // Remove doc
    yield Player.remove(player._id);
    // Commit change
    yield conn.commit();
    // close connection
    yield conn.close();
});

if (require.main === module) {
    main().finally(process.exit);
}
