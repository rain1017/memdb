'use strict';

var MongoBackend = require('./mongo-backend');
var MongooseBackend = require('./mongoose-backend');
var RedisBackend = require('./redis-backend');

var exports = {};

exports.create = function(backend, config){
	if(backend === 'mongodb'){
		return new MongoBackend(config);
	}
	else if(backend === 'mongoose'){
		return new MongooseBackend(config);
	}
	else if(backend === 'redis'){
		return new RedisBackend(config);
	}
	else{
		throw new Error('Invalid backend');
	}
};

module.exports = exports;
