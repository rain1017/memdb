# MemDB ![logo](https://github.com/memdb/memdb/wiki/images/logo.png)

[![Build Status](https://travis-ci.org/memdb/memdb.svg?branch=master)](https://travis-ci.org/memdb/memdb)
[![Dependencies Status](https://david-dm.org/memdb/memdb.svg)](https://david-dm.org/memdb/memdb)

### The world first distributed ACID transactional 'MongoDB'

- __Performance__ : In memory data access, up to 25,000 ops/shard (tested on EC2 c4.xlarge).

- __Horizontally Scalable__ : Performance grows linearly by adding more shards.

- __ACID Transaction__ : Full [ACID](https://en.wikipedia.org/wiki/ACID) transaction support on distributed environment.

- __MongoDB Compatible__ : It's just a 'MongoDB' with transaction support, built-in 'Mongoose' support. 

![memdbshell.gif](https://github.com/memdb/memdb/wiki/images/memdbshell.gif)

## [The Wiki](https://github.com/memdb/memdb/wiki)

## Quick Start

### Install Dependencies

* Install [Node.js](https://nodejs.org/download/)

* Install [Redis](http://redis.io/download)

* Install [MongoDB](https://www.mongodb.org/downloads)

### Install MemDB

* Install memdb
```
sudo npm install -g memdb-server
```

### Configure MemDB

Copy default config file `node_modules/memdb-server/memdb.conf.js` to ~/.memdb, and modify it on your need. Please read comments carefully.

### Start MemDB

Use `memdbcluster` to control lifecycle of memdb server cluster
```
memdbcluster [start | stop | status] [--conf=memdb.conf.js] [--shard=shardId]
```

### Play with memdb shell
See the top GIF, note how ACID transaction works.

### Nodejs Client with AutoConnection

```js
var memdb = require('memdb-client');
// just bluebird promise
var P = memdb.Promise;

var main = P.coroutine(function*(){
    // All database access should via this autoconn object, you can preserve autoconn object in a global module that can be accessed anywhere
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

To run the sample above
* Make sure you have started shard 's1' on localhost:31017, 's2' on localhost:31018.
* Install npm dependencies
```
npm install memdb-client
```
* run with node >= 0.12 with --harmony option
```
node --harmony sample.js
```

### Mdbgoose

Mdbgoose is a modified [Mongoose](http://mongoosejs.com) version that work for memdb

```js
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
        var doc = yield Player.findByIdAsync('p1');
        console.log('%j', doc);

        // find player by areaId, return array of players
        var docs = yield Player.findAsync({areaId : 1});
        console.log('%j', docs);

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
    main().finally(process.exit);
}
```

To run the sample above:
* Add the following index config in memdb.conf.js
```
collections : {
    player : {
        indexes : [
            {
                keys : ['areaId'],
            },
            {
                keys : ['deviceType', 'deviceId'],
                unique : true,
            },
        ]
    }
}
```
* Restart memdb cluster
```
memdbcluster stop
memdbcluster drop // drop existing data if database is not empty
memdbcluster start
```
* Make sure you have started shard 's1' on localhost:31017
* Install npm dependencies
```
npm install memdb-client
```
* Run with node >= 0.12 with --harmony option
```
node --harmony sample.js
```

### Tips
* Data is not bind to specified shard, you can access any data from any shard.
* All operations inside a single transaction must be executed on one single shard.
* Access the same data from the same shard if possible, which will maximize performance.

__Please read [The Wiki](https://github.com/memdb/memdb/wiki) for further reference__

## Contact Us
* [Github Issue](https://github.com/memdb/memdb/issues)
* Mailing list: [memdbd@googlegroups.com](https://groups.google.com/forum/#!forum/memdbd)
* Email: [memdbd@gmail.com](mailto:memdbd@gmail.com)

## License

Copyright 2015 The MemDB Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See the AUTHORS file
for names of contributors.
