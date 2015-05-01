'use strict';

var P = require('bluebird');

var Connection = require('./connection');
var AutoConnection = require('./autoconnection');

var _db = null;
var _autoconn = null; // autoconn to in-process server

var _autoconns = {}; // autoconns to socket.io server

// Start in-process db server
exports.startServer = function(opts){
	if(!_db){
		var Database = require('../app/database');
		_db = new Database(opts);
	}
	return _db.start();
};

// Stop in-process db server
exports.stopServer = function(){
	if(!_db){
		throw new Error('please startServer first')
	}
	return _db.stop()
	.then(function(){
		_db = null;
		_autoconn = null;
	});
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
	return P.try(function(){
		return conn.connect();
	})
	.then(function(){
		return conn;
	});
};

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
	return P.map(Object.keys(_autoconns), function(addr){
		return _autoconns[addr].close();
	});
};

Object.defineProperty(exports, 'goose', {
	get : function(){
		return require('./mdbgoose');
	},
});

Object.defineProperty(exports, 'logger', {
	get : function(){
		return require('pomelo-logger');
	},
});

Object.defineProperty(exports, 'Promise', {
	get : function(){
		return require('bluebird');
	},
});
