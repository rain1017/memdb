'use strict';

var P = require('bluebird');
var child_process = require('child_process');
var path = require('path');
var redis = P.promisifyAll(require('redis'));
var mongodb = P.promisifyAll(require('mongodb'));
var pomeloLogger = require('pomelo-logger');
var logger = pomeloLogger.getLogger('test', __filename);

var config = require('./memdb.json');

if(config.promise && config.promise.longStackTraces){
	P.longStackTraces();
}

if(config.logger && config.logger.level){
	pomeloLogger.setGlobalLogLevel(pomeloLogger.levels[config.logger.level]);
}

var flushdb = function(cb){
	return P.try(function(){
		return P.promisify(mongodb.MongoClient.connect)(config.backend.url, config.backend.options);
	})
	.then(function(db){
		return P.try(function(){
			return db.dropDatabaseAsync();
		})
		.then(function(){
			return db.closeAsync();
		});
	})
	.then(function(){
		var client = redis.createClient(config.redis.port, config.redis.host);
		client.select(config.redis.db);
		return client.flushdbAsync()
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

	return P.delay(1000) // wait for server start
	.then(function(){
		return serverProcess;
	});
};

var stopServer = function(serverProcess){
	if(!serverProcess){
		return;
	}
	var deferred = P.defer();
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
