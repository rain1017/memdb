'use strict';

var Q = require('q');
var Shard = require('./shard');
var Connection = require('./connection');
var AutoConnection = require('./autoconnection');

var exports = {};

var shardInstance = null;

exports.start = function(opts){
	if(shardInstance){
		throw new Error('memorydb already started');
	}
	shardInstance = new Shard(opts);
	shardInstance.on('stop', function(){
		//Auto restart
		shardInstance = null;
		opts._id = null; //Must use a different id
		exports.start(opts);
	});
	return shardInstance.start();
};

exports.stop = function(){
	shardInstance.removeAllListeners('stop');
	return Q.fcall(function(){
		return shardInstance.stop();
	})
	.then(function(){
		shardInstance = null;
	});
};

exports.connect = function(){
	if(!shardInstance){
		throw new Error('memorydb not started');
	}
	return new Connection({shard : shardInstance});
};

exports.autoConnect = function(){
	if(!shardInstance){
		throw new Error('memorydb not started');
	}
	return new AutoConnection({shard : shardInstance});
};

module.exports = exports;
