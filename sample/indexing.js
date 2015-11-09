// Copyright 2015 rain1017.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

'use strict';

// Add the following index config in memdb.conf.js and restart memdbcluster
//
// collections : {
//     player : {
//         indexes : [
//             {
//                 keys : ['areaId'],
//                 valueIgnore : {
//                     areaId : ['', -1],
//                 },
//             },
//             {
//                 keys : ['deviceType', 'deviceId'],
//                 unique : true,
//             },
//         ]
//     }
// },

// npm install memdb-client
// run with node >= 0.12 with --harmony option

// We assume you have started shard 's1' on localhost:31017

var memdb = require('memdb-client');
var P = memdb.Promise;
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
    main().finally(process.exit);
}
