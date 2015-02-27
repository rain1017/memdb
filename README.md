# memorydb

Distributed transactional in memory database

[![Build Status](https://travis-ci.org/rain1017/memorydb.svg?branch=master)](https://travis-ci.org/rain1017/memorydb)
[![Dependencies Status](https://david-dm.org/rain1017/memorydb.svg)](https://david-dm.org/rain1017/memorydb)

Geting the __performance__ of in memory database, the __scalibility__ of distributed database, and the __robustness__ of transactional database.

## Why memorydb?

* __Performance__ : Data access is mainly based on in process memory, which is extremely fast.

* __Scalable__ : System is horizontally scalable by adding more shards

* __Transaction/Locking__ : You can commit/rollback change just like traditional database. 'row' based locking is also supported.

### Which is suite for memorydb?

* Network intensitive realtime application
* Online Game, Chat/Push service, etc.

### Which is not suite for memorydb?

* Data critical application
* Application heavily relied on SQL

## Main Concepts

### Shard

* A shard is a node in the distributed system
* Each shard preserve a part of data (on demand) in local memory
* Client is in the same process with one of the shard (Each client must connect to one shard, and all request is via this shard)
* In process access if the requested data is already in the shard's local memory, otherwise the shard will load the data from backend.

### Backend

* Backend is center persistent storage, all data in the system is eventually persistented to backend db.
* You can choose mongodb or redis as backend.

### Collection

* One collection of documents, like mongodb's collection or mysql's table.

### Document

* One document, like mongodb's document or mysql's row.

### Connection

* A virtual 'connection' to database, like traditional database's connection.
* Connection is not concurrency safe (due to node's async nature), DO NOT share connection in different API handlers which may run concurrently. We suggest you use AutoConnection instead.

### AutoConnection

* Use one connection in each execution scope, and auto commit on execution complete and rollback on execution failure.
* You can put each API handler in one execution scope.

### Lock

* Lock is based on document
* One connection must hold the lock in order to write (insert/remove/update) to a doc
* All locks held by a connection will be released after commit or rollback
* A connection will always read the latest commited value (if not holding the lock), you must explicitly lock the doc first if you do non-atomic 'read and update' operation.

###	Commit/Rollback

* All changes (after last commit) is not visible to other connections until being commited
* All changes (after last commit) will be discarded after rollback or closing a connection without commit

### System failures

* Shard failure: If one shard fail (heartbeat timeout), data in that shard will lose the progress since last persistent point.
* Backend failure: Data persistent will fail, and data can't transfer between shards (Cross shard data request will fail). This can get rescued when backend is online.
* Redis failure: The cluster will hang, and you will lose the progress since last persistent point of each shard.

## Sample

```
var memorydb = require('memorydb');
var Q = require('q');
var should = require('should');

// memorydb's config
var config = {
	//shard Id (optional)
	_id : 'shard1',
	// Center backend storage, must be same for shards in the same cluster
	backend : 'mongodb',
	backendConfig : {uri : 'mongodb://localhost/memorydb-test'},
	// Used for backendLock, must be same for shards in the same cluster
	redisConfig : {host : '127.0.0.1', port : 6379},
};

var doc = {_id : 1, name : 'rain', level : 1};

var autoconn = null;
return Q.fcall(function(){
	// Start memorydb
	return memorydb.start(config);
})
.then(function(){
	// Get autoConnection
	autoconn = memorydb.autoConnect();

	return autoconn.execute(function(){
		// Get collection
		var User = autoconn.collection('user');
		return Q.fcall(function(){
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
	return autoconn.execute(function(){
		var User = autoconn.collection('user');
		return Q.fcall(function(){
			// Update one field
			return User.update(doc._id, {level : 2});
		}).then(function(){
			// Find specified field
			return User.find(doc._id, 'level')
			.then(function(ret){
				ret.should.eql({level : 2});
			});
		}).then(function(){
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
		var User = autoconn.collection('user');
		return Q.fcall(function(){
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
.then(function(){
	// Stop memorydb
	return memorydb.stop();
});

// For distributed system, just run memorydb in each server with the same config, and each server will be a shard.

```

### Best Practise

* Access same data from the same shard if possible, this will maximize the performance
* Access same data from different shards will cause the data to be synced between shards, which has huge performance penalty.

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
