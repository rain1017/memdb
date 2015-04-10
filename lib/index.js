'use strict';

var Q = require('q');
var Connection = require('./connection');
var AutoConnection = require('./autoconnection');

var _db = null;

// Start in-process db server
exports.startServer = function(opts){
	if(!!_db){
		throw new Error('server already started');
	}

	var Database = require('../app/database');
	_db = new Database(opts);
	return _db.start();
};

// Stop in-process db server
exports.stopServer = function(){
	if(!_db){
		throw new Error('server not started');
	}
	return _db.stop();
};

exports.connect = function(opts){
	opts = opts || {};
	if(!(opts.host && opts.port)){
		if(!_db){
			throw new Error('please startServer first');
		}
		opts.db = _db;
	}

	var conn = new Connection(opts);
	return Q.fcall(function(){
		return conn.connect();
	})
	.then(function(){
		return conn;
	});
};

var _autoconns = {}; // autoconns to socket.io server
var _autoconn = null; // autoconn to in-process server

exports.autoConnect = function(opts){
	opts = opts || {};
	if(!(opts.host && opts.port)){
		if(!_autoconn){
			if(!_db){
				throw new Error('please startServer first');
			}
			opts.db = _db;
			_autoconn = new AutoConnection(opts);
		}
		return _autoconn;
	}
	else{
		var addr = opts.host + ':' + opts.port;
		if(!_autoconns.hasOwnProperty(addr)){
			_autoconns[addr] = new AutoConnection(opts);
		}
		return _autoconns[addr];
	}
};

exports.close = function(){
	return Q.all(Object.keys(_autoconns).map(function(addr){
		return _autoconns[addr].close();
	}));
};

Object.defineProperty(exports, 'goose', {
	get : function(){
		return require('./mdbgoose');
	},
});
