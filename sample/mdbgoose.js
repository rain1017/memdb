'use strict';

// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

// First start memdb server by:
// node app/server.js --conf=test/memdb.json --shard=s1 -d

var memdb = require('memdb');
var mdbgoose = memdb.goose;
var P = memdb.Promise;
var should = require('should');

// Define player schema
var playerSchema = new mdbgoose.Schema({
    _id : String,
    name : String,
    areaId : {type : Number, index : true, indexIgnore : [-1]},
    deviceType : {type : Number, indexIgnore : [-1]},
    deviceId : {type : String, indexIgnore : ['']},
    items : [mdbgoose.SchemaTypes.Mixed],
}, {collection : 'player'});
// Define a compound unique index
playerSchema.index({deviceType : 1, deviceId : 1}, {unique : true});

// You can parse mdbgoose schema to memdb config
// config.collections = mdbgoose.genCollectionConfig();

// Define player model
var Player = mdbgoose.model('player', playerSchema);

var main = P.coroutine(function*(){
    // Connect to memdb
    yield mdbgoose.connectAsync({
        shards : {s1 : {host : '127.0.0.1', port: 31017}}
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
        doc._id.should.eql('p1');

        // find player by areaId, return array of players
        var docs = yield Player.findAsync({areaId : 1});
        docs.length.should.eql(1);
        docs[0].areaId.should.eql(1);

        // find player by deviceType and deviceId
        player = yield Player.findOneAsync({deviceType : 1, deviceId : 'id1'});

        // update player
        player.areaId = 2;
        yield player.saveAsync();

        // remove the player
        yield player.removeAsync();

    }), 's1');
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}
