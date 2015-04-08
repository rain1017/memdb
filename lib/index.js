'use strict';

var Q = require('q');
var Connection = require('./connection');
var AutoConnection = require('./autoconnection');

var DEFAULT_HOST = '127.0.0.1';
var DEFAULT_PORT = 3000;

var _autoconns = {};

exports.connect = function(host, port){
	if(!host){
		host = DEFAULT_HOST;
	}
	if(!port){
		port = DEFAULT_PORT;
	}

	var conn = new Connection({host : host, port : port});
	return Q.fcall(function(){
		return conn.connect();
	})
	.then(function(){
		return conn;
	});
};

exports.autoConnect = function(host, port){
	host = host || DEFAULT_HOST;
	port = port || DEFAULT_PORT;

	var addr = host + ':' + port;
	if(!_autoconns.hasOwnProperty(addr)){
		_autoconns[addr] = new AutoConnection({host : host, port : port});
	}

	return Q.fcall(function(){
		return _autoconns[addr];
	});
};

exports.close = function(){
	return Q.all(Object.keys(_autoconns).map(function(addr){
		return _autoconns[addr].close();
	}));
};

exports.goose = function(){
	var mdbgoose = require('./mdbgoose');
	return mdbgoose;
};
