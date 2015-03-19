'use strict';

var Q = require('q');
var DataBase = require('./database');
var Connection = require('./client/connection');
var AutoConnection = require('./client/autoconnection');
var exports = {};

var _db = null;
var _autoconn = null;

var ensureRunning = function(){
	if(!_db){
		throw new Error('memorydb not started');
	}
};

exports.start = function(opts){
	if(_db){
		throw new Error('memorydb already started');
	}
	_db = new DataBase(opts);
	return _db.start();
};

exports.stop = function(){
	ensureRunning();
	_db.removeAllListeners('stop');
	return Q.fcall(function(){
		return _db.stop();
	})
	.then(function(){
		_db = null;
		_autoconn = null;
	});
};

exports.connect = function(){
	ensureRunning();
	return new Connection(_db);
};

exports.autoConnect = function(){
	ensureRunning();
	if(!_autoconn) {
		_autoconn = new AutoConnection(_db);
	}
	return _autoconn;
};

exports.persistentAll = function(){
	ensureRunning();
	return _db.persistentAll();
};

exports.goose = function(){
	return require('./client/mdbgoose');
};

exports.configureLogger = function(){
	var pomeloLogger = require('pomelo-logger');
	pomeloLogger.configure.apply(pomeloLogger, [].slice.call(arguments));
};

module.exports = exports;
