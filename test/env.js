'use strict';

var child_process = require('child_process');
var path = require('path');
var redis = require('redis');
var mongodb = require('mongodb');
var logger = require('pomelo-logger').getLogger('test', __filename);
var Q = require('q');
Q.longStackSupport = true;

var config = require('./memdb.json');

var flushdb = function(cb){
	return Q.fcall(function(){
		return Q.ninvoke(mongodb.MongoClient, 'connect', config.backend.url, config.backend.options);
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
		var client = redis.createClient(config.redis.port, config.redis.host);
		return Q.nfcall(function(cb){
			client.flushdb(cb);
		})
		.then(function(){
			client.end();
		});
	})
	.then(function(){
		logger.info('flushed db');
	})
	.nodeify(cb);
};

var startServer = function(shardId){
	var confPath = path.join(__dirname, 'memdb.json');
	var serverScript = path.join(__dirname, '../app/server.js');
	var args = [serverScript, '--conf=' + confPath, '--shard=' + shardId];
	var serverProcess = child_process.spawn(process.execPath, args);

	// This is required! otherwise server will block due to stdout buffer full
	serverProcess.stdout.pipe(process.stdout);
	serverProcess.stderr.pipe(process.stderr);

	return Q() //jshint ignore:line
	.delay(1000) // wait for server start
	.then(function(){
		return serverProcess;
	});
};

var stopServer = function(serverProcess){
	if(!serverProcess){
		return;
	}
	var deferred = Q.defer();
	serverProcess.on('exit', function(code, signal){
		if(code === 0){
			deferred.resolve();
		}
		else{
			deferred.reject('server process returned non-zero code');
		}
	});
	serverProcess.kill();
	return deferred.promise;
};

module.exports = {
	config : config,
	flushdb : flushdb,
	startServer : startServer,
	stopServer : stopServer,

	dbConfig : function(shardId){
		return {
			shard : shardId,
			redis : config.redis,
			backend : config.backend,
			slave : config.shards[shardId].slave,
			collections : config.collections,
		};
	},
};
