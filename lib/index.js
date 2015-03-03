'use strict';

var Q = require('q');
var DataBase = require('./database');
var Connection = require('./client/connection');
var AutoConnection = require('./client/autoconnection');

var exports = {};

var db = null;

exports.start = function(opts){
	if(db){
		throw new Error('memorydb already started');
	}
	db = new DataBase(opts);
	db.on('stop', function(){
		//Auto restart
		db = null;
		opts._id = null; //Must use a different id
		exports.start(opts);
	});
	return db.start();
};

exports.stop = function(){
	db.removeAllListeners('stop');
	return Q.fcall(function(){
		return db.stop();
	})
	.then(function(){
		db = null;
	});
};

exports.connect = function(){
	if(!db){
		throw new Error('memorydb not started');
	}
	return new Connection(db);
};

var _autoconn = null;

exports.autoConnect = function(){
	if(!db){
		throw new Error('memorydb not started');
	}
	if(!_autoconn) {
		_autoconn = new AutoConnection(db);
	}
	return _autoconn;
};

module.exports = exports;
