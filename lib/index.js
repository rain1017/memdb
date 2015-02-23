'use strict';

var Shard = require('./shard');
var Connection = require('./connection');
var AutoConnection = require('./autoconnection');

var exports = {};

var shard = null;

exports.start = function(opts){
	if(shard){
		throw new Error('memorydb already started');
	}
	shard = new Shard(opts);
	return shard.start();
};

exports.stop = function(){
	return shard.stop();
};

exports.connect = function(){
	if(!shard){
		throw new Error('memorydb not started');
	}
	return new Connection({shard : shard});
};

exports.autoConnect = function(){
	if(!shard){
		throw new Error('memorydb not started');
	}
	return new AutoConnection({shard : shard});
};

module.exports = exports;
