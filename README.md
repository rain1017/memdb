# memdb

Distributed transactional in memory database

[![Build Status](https://travis-ci.org/rain1017/memdb.svg?branch=master)](https://travis-ci.org/rain1017/memdb)
[![Dependencies Status](https://david-dm.org/rain1017/memdb.svg)](https://david-dm.org/rain1017/memdb)

## Why memdb?

- [x] __Performance__ : Data access is mainly based on in process memory, which is extremely fast.

- [x] __Scalable__ : System is horizontally scalable by adding more shards.

- [x] __Transaction__ : Full transaction support like traditional database, data consistency is guaranteed. 'row' based locking mechanism is used.

- [x] __High Availability__ : Each shard is backed by one or more redis replica, you will never lose any commited data.

__Comparison with other databases__

Database | Performance      | Horizontally Scalable | Transaction Support | Data Structure  
---------|------------------|-----------------------|---------------------|-----------------
MySQL    | Medium (Disk I/O)| No                    | __Yes (InnoDB)__   | Row based       
MongoDB  | Medium (Disk I/O)| __Yes__                   | No  (except some basic atomic modifier) | __Object(BSON)__   
Redis    | __High (Memory)__ | __Yes__                   | No  (.multi can do some 'transaction like' thing) | Very Elemental  
__MemDB__    | __High (Memory)__ | __Yes__                   | __Yes__                 | __Object(JSON)__   

## Documents

### [The Wiki](https://github.com/rain1017/memdb/wiki)

## Quick Start

### Install Dependencies

* Install [Node.js v0.12](https://nodejs.org/download/)

* Install [Redis](http://redis.io/download)

* Install [MongoDB](https://www.mongodb.org/downloads)

### A Quick Sample

```javascript
var memdb = require('memdb');
var mdbgoose = memdb.goose;
var P = require('bluebird');

// memdb's config
var config = {
    //shard Id (Must unique and immutable for each shard)
    shard : 's1',
    // Global backend storage, all shards must connect to the same mongodb (or mongodb cluster)
    backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
    // Global locking redis, all shards must connect to the same redis (or redis cluster)
    locking : {host : '127.0.0.1', port : 6379, db : 0},
    // Global event redis, all shards must connect to the same redis
    event : {host : '127.0.0.1', port : 6379, db : 0},
    // Data replication redis, one redis instance for each shard
    slave : {host : '127.0.0.1', port : 6379, db : 1},
};

// Define player schema
var playerSchema = new mdbgoose.Schema({
    _id : String,
    name : String,
    areaId : {type : Number, index : true, indexIgnore : [-1, null]},
    deviceType : {type : Number, indexIgnore : [-1, null]},
    deviceId : {type : String, indexIgnore : ['', null]},
    items : [mdbgoose.SchemaTypes.Mixed],
}, {collection : 'player'});
// Define a compound unique index
playerSchema.index({deviceType : 1, deviceId : 1}, {unique : true});

// Define player model
var Player = mdbgoose.model('player', playerSchema);

var main = P.coroutine(function*(){
    // Parse mdbgoose schema to collection config
    config.collections = mdbgoose.genCollectionConfig();
    // Start a memdb shard with in-process mode
    yield memdb.startServer(config);

    // Connect to in-process server
    yield mdbgoose.connectAsync();
    // Execute in a transaction
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
        console.log(yield Player.findAsync('p1'));
        // find player by areaId, return array of players
        console.log(yield Player.findAsync({areaId : 1}));
        // find player by deviceType and deviceId
        player = yield Player.findOneAsync({deviceType : 1, deviceId : 'id1'});
        console.log(player);

        // update player
        player.areaId = 2;
        yield player.saveAsync();

        // remove the player
        yield player.removeAsync();
    }));

    // stop memdb server
    yield memdb.stopServer();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}
```

__Run the sample__
```
npm install memdb bluebird
node --harmony sample.js
```

__Become a cluster__

Just start more shards that connect to __the same global services (backend/locking/event)__, and they will automatically become a MemDB cluster.

## Quick-pomelo
__[quick-pomelo](http://quickpomelo.com)__ is a rapid and robust game server framework based on memdb

## License
(The MIT License)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
