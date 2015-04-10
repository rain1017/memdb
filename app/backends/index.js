'use strict';

var MongoBackend = require('./mongo-backend');
var RedisBackend = require('./redis-backend');

var exports = {};

exports.create = function(config){
	config = config || {};
	var engine = config.engine || 'mongodb';

	if(engine === 'mongodb'){
		return new MongoBackend(config);
	}
	else if(engine === 'redis'){
		return new RedisBackend(config);
	}
	else{
		throw new Error('Invalid backend engine');
	}
};

module.exports = exports;
