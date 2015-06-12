'use strict';

// npm install memdb-client
// run with node >= 0.12 with --harmony option

// We assure you have started shard 's1' on localhost:31017

var memdb = require('memdb-client');
var P = memdb.Promise;
var mdbgoose = memdb.goose;

// Define player schema
var playerSchema = new mdbgoose.Schema({
    _id : String,
    name : String,
    areaId : Number,
    deviceType : Number,
    deviceId : String,
    items : [mdbgoose.SchemaTypes.Mixed],
}, {collection : 'player'});
// Define player model
var Player = mdbgoose.model('player', playerSchema);

var main = P.coroutine(function*(){
    // Connect to memdb
    yield mdbgoose.connectAsync({
        shards : { // specify all shards here
            s1 : {host : '127.0.0.1', port: 31017},
            s2 : {host : '127.0.0.1', port: 31018},
        }
    });

    // Make a transaction in s1
    yield mdbgoose.transactionAsync(P.coroutine(function*(){

        var player = new Player({
            _id : 'p1',
            name: 'rain',
            areaId : 1,
            deviceType : 1,
            deviceId : 'id1',
            items : [],
        });

        // insert a player
        yield player.saveAsync();

        // find player by id
        var doc = yield Player.findAsync('p1');
        console.log('%j', doc);

        // find player by areaId, return array of players
        // index for areaId should be configured in memdb.json
        var docs = yield Player.findAsync({areaId : 1});
        console.log('%j', docs);

        // find player by deviceType and deviceId
        // a compound index should be configured in memdb.json
        player = yield Player.findOneAsync({deviceType : 1, deviceId : 'id1'});

        // update player
        player.areaId = 2;
        yield player.saveAsync();

        // remove the player
        yield player.removeAsync();

    }), 's1');
});

if (require.main === module) {
    main().finally(process.exit);
}
