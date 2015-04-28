'use strict';

var P = require('bluebird');
var util = require('util');
var sioClient = require('socket.io-client');
var logger = require('pomelo-logger').getLogger('memdb-client', __filename);

var Client = function(){
	this.socket = null;
	this.seq = 1;
	this.requests = {}; //{seq : deferred}
	this.domains = {}; //{seq : domain} saved domains
};

var proto = Client.prototype;

proto.connect = function(host, port){
	if(!!this.socket){
		throw new Error('already connected');
	}
	var self = this;

	var deferred = P.defer();

	var socket = sioClient.connect('http://' + host + ':' + port, {forceNew : true});
	socket.once('connect', function(){
		self.socket = socket;
		logger.info('connected to %s:%s', host, port);
		deferred.resolve();
	});
	socket.once('connect_error', function(e){
		logger.error('failed to connect %s:%s', host, port);
		deferred.reject(e);
	});

	socket.on('resp', function(msg){
		var level = msg.err ? 'warn' : 'info';
		logger[level]('%s:%s => %j', host, port, msg);
		var request = self.requests[msg.seq];
		if(!request){
			throw new Error('Request ' + msg.seq + ' not exist');
		}

		// restore saved domain
		process.domain = self.domains[msg.seq];

		if(!msg.err){
			request.resolve(msg.data);
		}
		else{
			request.reject(msg.err);
		}
		delete self.requests[msg.seq];
		delete self.domains[msg.seq];
	});

	return deferred.promise;
};

proto.disconnect = function(){
	if(!this.socket){
		throw new Error('not connected');
	}
	var self = this;

	var deferred = P.defer();

	this.socket.once('disconnect', function(){
		logger.info('disconnected from %s:%s', self.socket.io.opts.host, self.socket.io.opts.port);
		self.socket = null;
		self.requests = {};

		// Server will not disconnect if the client process exit immediately
		// So delay resolve promise
		setTimeout(function(){
			deferred.resolve();
		}, 1);
	});

	this.socket.disconnect();

	return deferred.promise;
};

proto.request = function(method, args){
	if(!this.socket){
		throw new Error('not connected');
	}

	var seq = this.seq++;

	var deferred = P.defer();
	this.requests[seq] = deferred;

	var msg = {
		seq : seq,
		method : method,
		args : args,
	};
	this.socket.emit('req', msg);

	// save domain
	this.domains[seq] = process.domain;

	logger.info('%s:%s <= %j', this.socket.io.opts.host, this.socket.io.opts.port, msg);
	return deferred.promise;
};

module.exports = Client;
