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

### Which is suite for memdb?

* Network intensitive realtime application
* Online Game, Chat/Push service, etc.

### Which is not suite for memdb?

* Application require complex SQL quering

## Main Concepts

### Shard

* A shard is a node in the distributed system
* Each shard preserve a part of data (on demand) in local memory
* The data is performed in local memory when requsted data is already in this shard, otherwise data will be synced between shards. You should access same data from the same shard if possible, this will maximize the performance

### Backend Persistent

* Backend is center persistent storage, all data in the system is eventually persistented to backend db.
* Mongodb is recommended for backend engine.

### Global Redis

* Internal use for database global locking/event mechanism

### Shard Redis Replication

Every shard use redis as data replication, you can add more replication to redis too. All commited data can be restored after server failure, you will never lose any commited data.

### Collection

* One collection of documents, like mongodb's collection or mysql's table.

### Document

* One document, like mongodb's document or mysql's row.

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

### In-process mode VS standalone mode

Memorydb support two running mode: in-process and standalone. 

* In-process mode: memdb is used as a library and started by library caller. Both client and server is in the same node process,which can maximize performance. You can use this mode as long as your client is written with node.js.
* Standalone mode: memdb is started as a socket server, the client should use socket to communicate with server (like other database). This mode is more flexible and support clients from other programming languages, but at a cost of performance penalty on network transfering. Use this mode when you need to access database from other programming languages or you need more flexibility on deployment.


## Sample

### The Basic

```
var memdb = require('memdb');
var P = require('bluebird');
var should = require('should');

// memdb's config
var config = {
	//shard Id (Must unique and immutable for each server)
	shard : 'shard1',
	// Center backend storage, must be same for all shards
	backend : {engine : 'mongodb', url : 'mongodb://localhost/memdb-test'},
	// Used for backendLock, must be same for all shards
	redis : {host : '127.0.0.1', port : 6379},
	// Redis data replication (for current shard)
	slave : {host : '127.0.0.1', port : 6379},
};

var player = {_id : 'p1', name : 'rain', level : 1};

var conn = null;

return P.try(function(){
	// Start memdb
	return memdb.startServer(config);
})
.then(function(){
	// Create a new connection
	return memdb.connect();
})
.then(function(ret){
	conn = ret;
	// Insert a doc to collection 'player'
	return conn.collection('player').insert(player._id, player);
})
.then(function(){
	// Find the doc
	return conn.collection('player').find(player._id)
	.then(function(ret){
		ret.should.eql(player);
	});
})
.then(function(){
	// Commit changes
	return conn.commit();
})
.then(function(){
	// Update a field
	return conn.collection('player').update(player._id, {$set : {level : 2}});
})
.then(function(){
	// Find the doc (only return specified field)
	return conn.collection('player').find(player._id, 'level')
	.then(function(ret){
		ret.level.should.eql(2);
	});
})
.then(function(){
	// Roll back changes
	return conn.rollback();
})
.then(function(){
	// Doc should rolled back
	return conn.collection('player').find(player._id, 'level')
	.then(function(ret){
		ret.level.should.eql(1);
	});
})
.then(function(){
	// Remove doc
	return conn.collection('player').remove(player._id);
})
.then(function(){
	// Commit changes
	return conn.commit();
})
.then(function(){
	// Doc should not exist
	return conn.collection('player').find(player._id)
	.then(function(ret){
		(ret === null).should.eql(true);
	});
})
.then(function(){
	// Close connection
	return conn.close();
})
.finally(function(){
	// Stop memdb
	return memdb.stopServer();
});

// For distributed system, just run memdb in each server with the same config, and each server will be a shard.

```

### AutoConnection

```
var memdb = require('memdb');
var P = require('bluebird');
var should = require('should');

var doc = {_id : '1', name : 'rain', level : 1};

var autoconn = null;

return P.try(function(){
	// Start memdb
	return memdb.startServer(config);
})
.then(function(){
	// Get autoConnection
	autoconn = memdb.autoConnect();

	// One transaction for each execution scope
	return autoconn.execute(function(){
		// Get collection
		var User = autoconn.collection('user');
		return P.try(function(){
			// Insert a doc
			return User.insert(doc._id, doc);
		})
		.then(function(){
			// find the doc
			return User.find(doc._id)
			.then(function(ret){
				ret.should.eql(doc);
			});
		});
	}); // Auto commit here
})
.then(function(){
	// One transaction for each execution scope
	return autoconn.execute(function(){
		// Get collection
		var User = autoconn.collection('user');
		return P.try(function(){
			// Update one field
			return User.update(doc._id, {$set : {level : 2}});
		})
		.then(function(){
			// Find specified field
			return User.find(doc._id, 'level')
			.then(function(ret){
				ret.should.eql({level : 2});
			});
		})
		.then(function(){
			// Exception here!
			throw new Error('Oops!');
		});
	}) // Rollback on exception
	.catch(function(e){
		e.message.should.eql('Oops!');
	});
})
.then(function(){
	return autoconn.execute(function(){
		// Get collection
		var User = autoconn.collection('user');
		return P.try(function(){
			// doc should be rolled back
			return User.find(doc._id, 'level')
			.then(function(ret){
				ret.should.eql({level : 1});
			});
		})
		.then(function(){
			// Remove the doc
			return User.remove(doc._id);
		}); // Auto commit here
	});
})
.then(function(){
	// Close autoConnection
	return autoconn.close();
})
.finally(function(){
	// Stop memdb
	return memdb.stopServer();
});

```

### Mdbgoose
```
var memdb = require('memdb');
var P = require('bluebird');
var should = require('should');

var mdbgoose = memdb.goose;
var Schema = mdbgoose.Schema;

var playerSchema = new Schema({
	_id : String,
	name : String,
	fullname : {first: String, last: String},
	extra : mdbgoose.SchemaTypes.mixed,
}, {collection : 'player', versionKey: false});

var Player = mdbgoose.model('player', playerSchema);

return P.try(function(){
	return memdb.startServer(config);
})
.then(function(){
	return mdbgoose.execute(function(){
		return P.try(function(){
			var player = new Player({
				_id : 'p1',
				name: 'rain',
				fullname : {firt : 'Yu', last : 'Xia'},
				extra : {},
			});
			return player.saveAsync();
		})
		.then(function(){
			return Player.findAsync('p1');
		})
		.then(function(player){
			player.name.should.eql('rain');
			return player.removeAsync();
		});
	});
})
.finally(function(){
	return memdb.stopServer();
});
```

### Standalone mode

```
/**
 * First, start memdb server manually
 *
 * node ./app/server.js --conf=./test/memdb.json --shard=s1
 */

var memdb = require('../lib');
var P = require('bluebird');
var should = require('should');

var autoconn = null;
return P.try(function(){
	// Connect to server, specify host and port
	return memdb.autoConnect({host : '127.0.0.1', port : 3000});
})
.then(function(ret){
	autoconn = ret;

	return autoconn.execute(function(){
		var Player = autoconn.collection('player');
		return P.try(function(){
			return Player.insert(1, {name : 'rain'});
		})
		.then(function(){
			return Player.find(1);
		})
		.then(function(player){
			player.name.should.eql('rain');
			return Player.remove(1);
		});
	});
})
.then(function(){
	// Close connection
	return autoconn.close();
});

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
