'use strict';

var path = require('path');
var util = require('util');
var mongoose = require('./lib/mdbgoose');
var types = mongoose.SchemaTypes;

var memorydb = require('./lib');
var uuid = require('node-uuid');
var Q = require('q');
var should = require('should');
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

Q.longStackSupport = true;


// For distributed system, just run memorydb in each server with the same config, and each server will be a shard.
var main = function(){

	// memorydb's config
	var config = {
		//shard Id (optional)
		_id : 'shard1',
		// Center backend storage, must be same for shards in the same cluster
		backend : 'mongoose',
		backendConfig : {uri : 'mongodb://localhost/memorydb-test'},
		// Used for backendLock, must be same for shards in the same cluster
		redisConfig : {host : '127.0.0.1', port : 6379},
	};

	var doc = {_id : 1, name : 'rain', level : 1};

	var autoconn = null;
	return Q.fcall(function(){
		// Start memorydb
		logger.debug('start memorydb');
		return memorydb.start(config);
	})
	.then(function(){
		autoconn = memorydb.autoConnect();

		var id = null;
		var Cat = mongoose.model('Cat', { _id: String, name: String, fullname: {first: String, second: String}, extra: types.Mixed});
		Cat.findOne(function(err, doc){
			logger.debug('test findOne: doc=%j, err=', doc, err);
		});
		Cat.findById('54f3fb251444dd96c0afbd6e', 'name', function(err, doc){
			logger.debug('test findById: doc=%j, err=', doc, err);
		});
		return autoconn.execute(function(){
			var kitty = new Cat({ _id: uuid.v4(), name: 'Zildjian', fullname: {first: 'firstname', second: 'second name'}, extra: {xx: 'extra val'} });
			id = kitty.id;
			return Q.ninvoke(kitty, 'save')
			.then(function(){
				logger.info('save callback arguments: ' + util.inspect(arguments));
			})
			.then(function(){
				kitty.name = 'xxx';
				kitty.fullname.first = 'firstxxx';
				kitty.extra = {xx1: 'changed extra'};
				return Q.ninvoke(kitty, 'save');
			})
			.then(function(){
				return Q.nfcall(function(cb){
					return Cat.findOneInMdb(id, cb);
				}).then(function(ret){
					logger.info('findOneInMdb kitty: %j', ret);
				});
			});
		})
		.catch(function(e){
			logger.error('error caught: ', e);
		})
		.then(function(){
			return autoconn.execute(function(){
				return Q.nfcall(function(cb){
					return Cat.findOneInMdb(id, cb);
				})

			})
			.catch(function(e){
				logger.error('error caught: ', e);
			});
		});
	})
	.catch(function(e){
		logger.error('xxx error caught: ', e);
	})
	.then(function(){
		// Close autoConnection
		return Q.all([autoconn.close(), memorydb.stop()]);
	});
};

if (require.main === module) {
	return Q.fcall(function(){
		return main();
	})
	.fin(function(){
		process.exit();
	});
}
else {
	var A = function(){}
	A.prototype.toString = function(){return 'a'};

	var o = {};
	o[new A()] = 'adsf';
	logger.info('%j', o);

	Q.nfcall(function(cb){
		setTimeout(function(){
			console.log('1');
			cb();
		}, 1000);
	})
	.then(function(){
		return Q.nfcall(function(cb){
			setTimeout(function(){
				console.log('2');
				throw new Error('error raised');
				cb();
			}, 1000);
		});
	})
	.done(function(){
		console.log('q done');
	}
	,function(e){
		logger.error('error caught: ', e);
	})
	;

	/*
	var mongoose = require('mongoose');
	mongoose.connect('mongodb://localhost/memorydb-test');
	var Cat = mongoose.model('Cat', { _id: String, name: String, fullname: {first: String, second: String}, extra: types.Mixed});
	Cat.findOne(function(err, doc){
		logger.debug('test findOne: doc=%j, err=', doc, err);
	});
	Cat.findById('54f3fb251444dd96c0afbd6e', 'name', function(err, doc){
		logger.debug('test findById: doc=%j, err=', doc, err);
	});
	setTimeout(function(){mongoose.disconnect();}, 5000);
	*/
}
