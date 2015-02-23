'use strict';

var redis = require('redis');
var mongodb = require('mongodb');
var logger = require('pomelo-logger').getLogger('test', __filename);
var Q = require('q');
Q.longStackSupport = true;

var redisConfig = {host : '127.0.0.1', port : 6379};
var mongoConfig = {uri: 'mongodb://localhost/memorydb-test', options : {}};

var flushdb = function(cb){
	Q.fcall(function(){
		return Q.ninvoke(mongodb.MongoClient, 'connect', mongoConfig.uri, mongoConfig.options);
	})
	.then(function(db){
		return Q.fcall(function(){
			return Q.ninvoke(db, 'dropDatabase');
		})
		.then(function(){
			return Q.ninvoke(db, 'close');
		});
	})
	.then(function(){
		var client = redis.createClient(redisConfig.port, redisConfig.host);
		return Q.nfcall(function(cb){
			client.flushdb(cb);
		})
		.then(function(){
			client.end();
		});
	})
	.then(function(){
		logger.info('flushed db');
		cb();
	})
	.catch(cb);
};

module.exports = {
	redisConfig : redisConfig,
	mongoConfig : mongoConfig,
	flushdb : flushdb,
};
