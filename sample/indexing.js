'use strict';

// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

// First start memdb server by:
// memdbd --shard=s1

// Note that indexes is configured in memdb.json

var memdb = require('memdb');
var P = require('bluebird');
var should = require('should');

var main = P.coroutine(function*(){
    // Connect to memdb
    var autoconn = yield memdb.autoConnect({
        shards : {s1 : {host : '127.0.0.1', port : 31017}}
    });

    var Player = autoconn.collection('player');

    // make transaction in shard s1
    yield autoconn.transaction(P.coroutine(function*(){
        // Insert players
        var players = [{_id : 'p1', name : 'rain', areaId : 1},
                       {_id : 'p2', name : 'snow', areaId : 2}];
        yield Player.insert(players);

        // Find all players in area1
        var docs = yield Player.find({areaId : 1});
        docs.length.should.eql(1);
        docs[0].areaId.should.eql(1);

        // Find doc of id p1, return one doc
        var doc = yield Player.find('p1');
        doc._id.should.eql('p1');

        // DO NOT do this! Error will be thrown since name is not indexed.
        // yield Player.find({name : 'rain'});

        // Move all players in area1 into area2
        yield Player.update({areaId : 1}, {$set : {areaId : 2}});
        // Remove all players in area2
        yield Player.remove({areaId : 2});
    }), 's1');

    // make transaction in shard s1
    yield autoconn.transaction(P.coroutine(function*(){
        // Insert a player
        yield Player.insert({_id : 'p1', deviceType : 1, deviceId : 'id1'});

        // Find with compound key
        var docs = yield Player.find({deviceType : 1, deviceId : 'id1'});
        docs.length.should.eql(1);
        docs[0]._id.should.eql('p1');

        // Will throw duplicate key error
        // yield Player.insert({deviceType : 1, deviceId : 'id1'});

        // Remove player
        yield Player.remove('p1');
    }), 's1');
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}
