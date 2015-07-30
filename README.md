# memdb

Distributed transactional in memory database

[![Build Status](https://travis-ci.org/memdb/memdb.svg?branch=master)](https://travis-ci.org/memdb/memdb)
[![Dependencies Status](https://david-dm.org/memdb/memdb.svg)](https://david-dm.org/memdb/memdb)

## Why memdb?

- [x] __Performance__ : In memory data access which is extremely fast.

- [x] __Scalable__ : System is horizontally scalable by adding more shards.

- [x] __Transaction__ : Full transaction support like traditional database, data consistency is guaranteed. 'row' based locking mechanism is used.

- [x] __Simple__ : It's just a 'mongodb' with transaction support.

__Comparison with other databases__

Database | Performance      | Horizontally Scalable | Transaction Support | Data Structure  
---------|------------------|-----------------------|---------------------|-----------------
MySQL    | Medium (Disk I/O)| No                    | __Yes (InnoDB)__   | Row based       
MongoDB  | Medium (Disk I/O)| __Yes__                   | No  (except some basic atomic modifier) | __Object(BSON)__   
Redis    | __High (Memory)__ | __Yes__                   | No  (.multi can do some 'transaction like' thing) | Very Elemental  
__MemDB__    | __High (Memory)__ | __Yes__                   | __Yes__                 | __Object(JSON)__   

## Documents

### [The Wiki](https://github.com/memdb/memdb/wiki)

## Quick Start

### Install Dependencies

* Install [Node.js >=v0.10](https://nodejs.org/download/)

* Install [Redis](http://redis.io/download)

* Install [MongoDB](https://www.mongodb.org/downloads)

### Install MemDB

* Install memdb
```
sudo npm install -g memdb-server
```

### Configure MemDB

Modify settings in `.memdb.js` on your need. Please read comments carefully.

### Start MemDB

Use `memdbcluster` to control lifecycle of memdb server cluster
```
memdbcluster [start | stop | status] [--conf=.memdb.js] [--shard=shardId]
```

### Play with memdb shell

```js
$ memdb -h 127.0.0.1 -p 31017 // specify the shard's host and port to connect
MemDB shell
connected to 127.0.0.1:31017
memdb> db.insert('player', {_id : 1, name : 'rain'}) // insert a doc to 'player' collection
'1'
memdb> db.find('player', 1)  // find doc by id
{ _id: '1', name: 'rain' }
memdb> db.commit() // commit changes
true
memdb> db.update('player', 1, {$set : {name : 'snow'}}) // update doc
1
memdb> db.find('player', 1, 'name')
{ name: 'snow' }
memdb> db.rollback() // rollback changes
true
memdb> db.find('player', 1, 'name')
{ name: 'rain' }
memdb> db.remove('player', 1) // remove doc
1
memdb> db.commit()
true
memdb> ^D (to exit)
```

### Nodejs client using AutoConnection

AutoConnection manages a pool of connections for each shard, execute transaction on specified shard, and auto commit on transaction complete or rollback on failure.

```js
// To run the sample:
// npm install memdb-client
// run with node >= 0.12 with --harmony option
// We assume you have started shard 's1' on localhost:31017, 's2' on localhost:31018.

var memdb = require('memdb-client');
var P = memdb.Promise; // just bluebird promise

var main = P.coroutine(function*(){
    // All database access should via this autoconn object, 
    // you can preserve autoconn object in a global module that can be accessed anywhere
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
```

### Nodejs client using MdbGoose

Mdbgoose is the 'mongoose' for memdb

```js
// To run the sample:
// npm install memdb-client
// run with node >= 0.12 with --harmony option
// We assume you have started shard 's1' on localhost:31017

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
        // (index should be configured in .memdb.js)
        var docs = yield Player.findAsync({areaId : 1});
        console.log('%j', docs);

        // find player by deviceType and deviceId
        // (index should be configured in .memdb.js)
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
```

## Quick-pomelo
__[quick-pomelo](http://quickpomelo.com)__ is a rapid and robust game server framework based on memdb


## License

MemDB - distributed transactional in memory database

Copyright (C) memdb

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
