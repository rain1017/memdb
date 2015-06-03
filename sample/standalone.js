'use strict';

// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var should = require('should');
var P = require('bluebird');

// IMPORTANT: You should first start memdb server by:
// node app/server.js --conf=test/memdb.json --shard=s1
// node app/server.js --conf=test/memdb.json --shard=s2

var main = P.coroutine(function*(){
    // When specifying shards config, client will connect to the standalone server
    var shards = {
            s1 : {host : '127.0.0.1', port : 3000},
            s2 : {host : '127.0.0.1', port : 3001},
        };
    var autoconn = yield memdb.autoConnect({shards : shards});

    var Player = autoconn.collection('player');
    var playerId = null;

    // Make a transaction in shard s1
    yield autoconn.transaction(P.coroutine(function*(){
        playerId = yield Player.insert({name : 'rain'});
    }), 's1');

    // Make another transaction in shard s2
    yield autoconn.transaction(P.coroutine(function*(){
        var player = yield Player.find(playerId);
        player.should.eql({_id : playerId, name : 'rain'});

        yield Player.remove(playerId);
    }), 's2');

    // Close the connection
    yield autoconn.close();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}
