# memdb

Distributed transactional in memory database

[![Build Status](https://travis-ci.org/rain1017/memdb.svg?branch=master)](https://travis-ci.org/rain1017/memdb)
[![Dependencies Status](https://david-dm.org/rain1017/memdb.svg)](https://david-dm.org/rain1017/memdb)

Geting the __performance__ of in memory database, the __scalibility__ of distributed database, and the __robustness__ of transactional database.

## Why memdb?

* __Performance__ : Data access is mainly based on in process memory, which is extremely fast.

* __Scalablility__ : System is horizontally scalable by adding more shards.

* __Transaction__ : You can commit/rollback change just like traditional database. 'row' based locking is also supported.

* __High Availability__ : All server is backed by one or more redis replication, you will never lose any commited data.

### Comparsion with other databases

Database | Performance      | Horizontally Scalable | Transaction Support | Data Structure  | High Availibility
---------|------------------|-----------------------|---------------------|-----------------|------------------
MySQL    | Medium (Disk I/O)| No                    | Yes (InnoDB)        | Row based       | Yes
MongoDB  | Medium (Disk I/O)| Yes                   | No                  | Object (Bson)   | Yes
Redis    | High (Memory)    | Yes                   | No                  | Very Basic      | Yes
MemDB    | High (Memory)    | Yes                   | Yes                 | Object (Json)   | Yes

### Which is suite for memdb?

* Network intensitive realtime application
* Application that has data hot area (Some data is tend to be accessed together in a certain period), like online game server

### Which is not suite for memdb?

* Application require complex SQL quering

## Prerequistics

### Node.js >=v0.10 with Promise

We assume you're already familiar with node.js. 

In addition, you should have some understanding on promise based async programming, see [bluebird](https://github.com/petkaantonov/bluebird) for detail. 

Some samples require node >= v0.12 with --harmony option, although this is not required by memdb, we strongly recommend you use generator in client code.

### redis

You should have have redis installed

### mongodb

You should have mongodb installed. Understand mongodb can help you get quick started, since many concepts in memdb is quite similiar with mongodb.

## System Components

### Shard

* A shard is a node in the distributed system
* Each shard preserve a part of data (on demand) in local memory
* The data is performed in local memory when requsted data is already in this shard, otherwise data will be synced between shards. You should access same data from the same shard if possible, this will maximize the performance

### Center Backend

* Backend is center persistent storage, all data in the system is eventually persistented to backend db.
* Mongodb is recommended for backend engine.

### Center Redis

* Internal use for database global locking/event mechanism

#### Shard Redis Replication

Every shard use redis as data replication, you can add more replication to redis too. All commited data can be restored after server failure, you will never lose any commited data.

## Database Concepts

### Document

* One document, like mongodb's document or mysql's row.
* Document is just json, so data in document must be json serializable. Number, String, Boolean, Array and Dict are supported, other type is not supported.

### Collection

* One collection of documents, like mongodb's collection or mysql's table.

### Connection

* A 'connection' to shard, like traditional database's connection.
* Connection is not concurrency safe (due to node's async nature), DO NOT share connection in different API handlers which may run concurrently. We suggest you use AutoConnection instead.

### AutoConnection

* Use one connection in each execution scope, and auto commit on execution complete and rollback on execution failure.
* Please put each request handler in one execution scope, therefore the request will be processed in one connection and guarded with transaction.

### Mdbgoose

Mdbgoose is modified from mongoose. mdbgoose for memdb is similar to mongoose for mongodb. You can leverage the power of mongoose for object modeling. Just use it like you were using mongoose!

### Concurrency/Locking/Transaction

The concurrency behavior is similar to mysql with innodb engine

* Lock is based on document
* One connection must hold the lock in order to write (insert/remove/update) to a doc
* All locks held by a connection will be released after commit or rollback
* A connection will always read the latest commited value (if not holding the lock), you must explicitly lock the doc first if you do non-atomic 'read and update' operation.
* All changes (after last commit) is not visible to other connections until being commited
* All changes (after last commit) will be discarded after rollback or closing a connection without commit
* If any error accured, the entire transaction will be rolledback immediately.

### Query and index

Memdb use hash based index. 

Each query must have some index to use (except by _id), query not using index is treated as error.

Compound index or unique index is supported.

Value comparing (like sorting, $lt, $gt) is not supported.

The doc count which has same index value is limited, so index on 'boolean' or 'enum' is not a good idea.

See [more]() about index

### In-process mode VS standalone mode

Memorydb support two running mode: in-process and standalone. 

#### In-process mode

memdb is used as a library and started by library caller. Both client and server is in the same node process, which can maximize performance. You can use this mode as long as your client is written with node.js. This mode is recommended.

#### Standalone mode

memdb is started as a socket server, the client should use socket to communicate with server (like other database). This mode is more flexible and support clients from other programming languages, but at a cost of performance penalty on network transfering. Use this mode when you need to access database from other programming languages or you need more flexibility on deployment.


## Sample

### The Basic

```
// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var P = require('bluebird');

// memdb's config
var config = {
    //shard Id (Must unique and immutable for each shard)
    shard : 's1',
    // Center backend storage, must be same for all shards
    backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
    // Center redis used for backendLock, must be same for all shards
    redis : {host : '127.0.0.1', port : 6379},
    // Redis data replication (for current shard)
    slave : {host : '127.0.0.1', port : 6379, db : 1},
};

var main = P.coroutine(function*(){
    // Start a memdb shard with in-process mode
    yield memdb.startServer(config);

    // Create a new connection
    var conn = yield memdb.connect();
    // Get player collection
    var Player = conn.collection('player');
    // Insert a doc
    var player = {_id : 'p1', name : 'rain', level : 1};
    yield Player.insert(player);
    // Commit changes
    yield conn.commit();
    // Update a field
    yield Player.update(player._id, {$set : {level : 2}});
    // Find the doc (only return specified field)
    console.log(yield Player.find(player._id, 'level')); // should print {level : 2}
    // Rollback changes
    yield conn.rollback();
    // Data restore to last commited state
    console.log(yield Player.find(player._id, 'level')); // should print {level : 1}
    // Remove doc
    yield Player.remove(player._id);
    // Commit change
    yield conn.commit();
    // close connection
    yield conn.close();

    // stop memdb server
    yield memdb.stopServer();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}

// For distributed system, just run memdb in each server with the same config, and each server will be a shard.

```

### AutoConnection

```
// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var P = require('bluebird');

// memdb's config
var config = {
    //shard Id (Must unique and immutable for each shard)
    shard : 's1',
    // Center backend storage, must be same for all shards
    backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
    // Center redis used for backendLock, must be same for all shards
    redis : {host : '127.0.0.1', port : 6379},
    // Redis data replication (for current shard)
    slave : {host : '127.0.0.1', port : 6379, db : 1},
};

var main = P.coroutine(function*(){
    // Start a memdb shard with in-process mode
    yield memdb.startServer(config);

    var autoconn = yield memdb.autoConnect();

    var User = autoconn.collection('user');
    var doc = {_id : '1', name : 'rain', level : 1};

    // Start a transaction
    yield autoconn.transaction(P.coroutine(function*(){
        // Insert a doc
        yield User.insert(doc);
        // Find the doc
        console.log(yield User.find(doc._id));
    })); // Auto commit after transaction

    try{
        // Start another transaction
        yield autoconn.transaction(P.coroutine(function*(){
            // Update doc with $set modifier
            yield User.update(doc._id, {$set : {level : 2}});
            // Find the changed doc
            console.log(yield User.find(doc._id));
            // Exception here!
            throw new Error('Oops!');
        }));
    }
    catch(err){
        // Catch the exception
        console.log(err);

        // Change is rolled back
        yield autoconn.transaction(P.coroutine(function*(){
            console.log(yield User.find(doc._id));
        }));
    }

    yield autoconn.transaction(P.coroutine(function*(){
        yield User.remove(doc._id);
    }));

    yield memdb.stopServer();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}

```

### Index
```
// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var P = require('bluebird');

// memdb's config
var config = {
    //shard Id (Must unique and immutable for each shard)
    shard : 's1',
    // Center backend storage, must be same for all shards
    backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
    // Center redis used for backendLock, must be same for all shards
    redis : {host : '127.0.0.1', port : 6379},
    // Redis data replication (for current shard)
    slave : {host : '127.0.0.1', port : 6379, db : 1},

    // Config for each collections
    collections : {
        // Config for player collection
        player : {
            // Index config
            indexes : [
                // index for key [areaId]
                {
                    keys : ['areaId'],
                    // Values that exclude from index
                    // Since some default value occurs too often, which can make index too large
                    valueIgnore : {
                        areaId : ['', -1, null],
                    }
                },
                // Unique index for compound keys [deviceType, deviceId]
                {
                    keys : ['deviceType', 'deviceId'],
                    unique : true,
                }
            ]
        }
    }
};

var main = P.coroutine(function*(){
    // Start a memdb shard with in-process mode
    yield memdb.startServer(config);

    // Connect to memdb
    var autoconn = yield memdb.autoConnect();
    var Player = autoconn.collection('player');

    yield autoconn.transaction(P.coroutine(function*(){
        // Insert players
        var players = [{_id : 1, name : 'rain', areaId : 1},
                       {_id : 2, name : 'snow', areaId : 2}];
        yield Player.insert(players);
        // Find all players in area1
        console.log(yield Player.find({areaId : 1}));
        // Also ok, but return one doc rather than array of docs
        console.log(yield Player.find({_id : 1}));
        // DO NOT do this! Error will be thrown since name is not indexed.
        // yield Player.find({name : 'rain'});

        // Move all players in area1 into area2
        yield Player.update({areaId : 1}, {$set : {areaId : 2}});
        // Remove all players in area2
        yield Player.remove({areaId : 2});
    }));

    yield autoconn.transaction(P.coroutine(function*(){
        // Insert a player
        yield Player.insert({_id : 1, deviceType : 1, deviceId : 'id1'});

        // Find with compound key
        console.log(yield Player.find({deviceType : 1, deviceId : 'id1'}));

        // Will throw duplicate key error
        // yield Player.insert({deviceType : 1, deviceId : 'id1'});

        // Remove player
        yield Player.remove(1);
    }));

    // stop memdb server
    yield memdb.stopServer();
});

if (require.main === module) {
    main().catch(console.error).finally(process.exit);
}
```

### Mdbgoose
```
// npm install memdb, bluebird
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var mdbgoose = memdb.goose;
var P = require('bluebird');

// memdb's config
var config = {
    //shard Id (Must unique and immutable for each shard)
    shard : 's1',
    // Center backend storage, must be same for all shards
    backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
    // Center redis used for backendLock, must be same for all shards
    redis : {host : '127.0.0.1', port : 6379},
    // Redis data replication (for current shard)
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

    // Execute in a transaction
    yield mdbgoose.transaction(P.coroutine(function*(){
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

### Standalone mode

```
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

```

## Performance Metric

The following data is tested on Intel Xeon 2.9G, Ubuntu 14.04, Node v0.10.33, using in-process mode

Test item   | Rate
----------- | -----------
Transaction inside one shard | 2500/s
Query (simple update) inside one shard | 50000/s
Across shard access to one document | 400/s

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
