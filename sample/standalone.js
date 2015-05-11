'use strict';

// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var P = require('bluebird');

// IMPORTANT: You should first start memdb server by:
// node app/server.js --conf=test/memdb.json --shard=s1

var main = P.coroutine(function*(){
    // When specifying port and host, client will connect to the standalone server
    var autoconn = yield memdb.autoConnect({host : '127.0.0.1', port : 3000});

    // Make some query in one transaction
    yield autoconn.transaction(P.coroutine(function*(){
        var Player = autoconn.collection('player');
        var playerId = yield Player.insert({name : 'rain'});
        console.log(yield Player.find(playerId));
        yield Player.remove(playerId);
    }));

    // Close the connection
    yield autoconn.close();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}
