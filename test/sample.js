'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var memorydb = require('../lib');

var main = function(){
	var opts = {
		backend : 'mongodb',
		backendConfig : {uri : 'mongodb://localhost'}
	};

	var client = null;

	var id = 1, doc = {key : 0, key2 : 'string'};

	return Q.fcall(function(){
		return memorydb.start(opts);
	}).then(function(){
		client = memorydb.autoConnect();
	}).then(function(){
		client.execute(function(){
			return client.collection('test').insert(id, doc);
		});
	}).then(function(){
		var concurrency = 8;

		return Q.all(_.range(concurrency).map(function(){
			// Simulate non-atomic check and update operation
			// each 'thread' add 1 to doc.key
			return client.execute(function(){
				var collection = client.collection('test');
				var value = null;
				return Q() // jshint ignore:line
				.delay(_.random(10))
				.fcall(function(){
					return collection.lock(id);
				}).then(function(){
					return collection.find(id, 'key');
				}).then(function(doc){
					value = doc.key;
				})
				.delay(_.random(20))
				.then(function(){
					return collection.update(id, {key : value + 1});
				});
			});

		})).then(function(){
			return client.execute(function(){
				var collection = client.collection('test');
				return Q.fcall(function(){
					return collection.find(id);
				}).then(function(doc){
					// value should equal to concurrency
					doc.key.should.equal(concurrency);

					return collection.remove(id);
				});
			});
		});

	}).then(function(){
		return client.execute(function(){
			return Q.fcall(function(){
				return client.collection('test').insert(id, doc);
			}).then(function(){
				//Should roll back on exception
				throw new Error('Oops!');
			});
		});
	})
	.fin(function(){
		return memorydb.stop();
	});
};

if (require.main === module) {
    main();
}
