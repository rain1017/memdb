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

## Quick Start

### Install Dependencies

* Install [Node.js v0.12](https://nodejs.org/download/)

* Install [Redis](http://redis.io/download)

* Install [MongoDB](https://www.mongodb.org/downloads)

### A Quick Sample

```
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
```

__Run the sample__
```
npm install memdb bluebird
node --harmony sample.js
```

__Become a cluster__

Just start more shards with __same backend and redis config__, and they will automatically become a MemDB cluster.



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
