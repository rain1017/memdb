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

First start memdb cluster by:
```
memdbd --conf=test/memdb.json --shard=s1
memdbd --conf=test/memdb.json --shard=s2
```

```js
// npm install memdb, should
// run with node >= 0.12 with --harmony option

var memdb = require('memdb');
var P = memdb.Promise;
var should = require('should');

var main = P.coroutine(function*(){

    var autoconn = yield memdb.autoConnect({
        shards : {
            s1 : {host : '127.0.0.1', port : 31017},
            s2 : {host : '127.0.0.1', port : 31018},
        }
    });

    var User = autoconn.collection('user');
    var doc = {_id : '1', name : 'rain', level : 1};

    // Make a transaction in shard s1
    yield autoconn.transaction(P.coroutine(function*(){
        // Remove if exist
        yield User.remove(doc._id);

        // Insert a doc
        yield User.insert(doc);
        // Find the doc
        yield User.find(doc._id);
        ret.level.should.eql(1);
    }), 's1'); // Auto commit after transaction

    try{
        // Make another transaction in shard s1
        yield autoconn.transaction(P.coroutine(function*(){
            // Update doc with $set modifier
            yield User.update(doc._id, {$set : {level : 2}});
            // Find the changed doc
            var ret = yield User.find(doc._id);
            ret.level.should.eql(2);
            // Exception here!
            throw new Error('Oops!');
        }), 's1');
    }
    catch(err){ // Catch the exception
        // Change is rolled back
        yield autoconn.transaction(P.coroutine(function*(){
            var ret = yield User.find(doc._id);
            ret.level.should.eql(1);
        }), 's1');
    }

    // Make transcation in another shard
    yield autoconn.transaction(P.coroutine(function*(){
        yield User.remove(doc._id);
    }), 's2');

    yield autoconn.close();
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
