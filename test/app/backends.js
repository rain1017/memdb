'use strict';

var Q = require('q');
var should = require('should');
var backends = require('../../app/backends');
var env = require('../env');

describe('backends test', function(){
	beforeEach(env.flushdb);
	after(env.flushdb);

	var testFunc = function(backend){
		var item1 = {name : 'test', id : 1, doc : {_id : 1, k : 1}};
		var item2 = {name : 'test2', id : 1, doc : {_id : 1, k2 : 1}};

		return Q.fcall(function(){
			return backend.start();
		})
		.then(function(){
			return backend.setMulti([item1, item2]);
		})
		.then(function(){
			return backend.get(item1.name, item1.id)
			.then(function(ret){
				ret.should.eql(item1.doc);
			});
		})
		.then(function(){
			return backend.get(item2.name, item2.id)
			.then(function(ret){
				ret.should.eql(item2.doc);
			});
		})
		.then(function(){
			// update item1
			item1.doc.k = 2;
			// remove item2
			item2.doc = null;
			return backend.setMulti([item1, item2]);
		})
		.then(function(){
			return backend.get(item1.name, item1.id)
			.then(function(ret){
				ret.should.eql(item1.doc);
			});
		})
		.then(function(){
			return backend.get(item2.name, item2.id)
			.then(function(ret){
				(ret === null).should.be.true; // jshint ignore:line
			});
		})
		.then(function(){
			return backend.drop('test');
		})
		.then(function(){
			return backend.drop();
		})
		.fin(function(){
			return backend.stop();
		});
	};

	it('mongo backend', function(cb){
		var opts = {
			engine : 'mongodb',
			url : env.config.backend.url,
		};
		var backend = backends.create(opts);
		return Q.fcall(function(){
			return testFunc(backend);
		})
		.nodeify(cb);
	});

	it('redis backend', function(cb){
		var opts = {
			engine : 'redis',
			host : env.config.redis.host,
			port : env.config.redis.port,
		};
		var backend = backends.create(opts);
		return Q.fcall(function(){
			return testFunc(backend);
		})
		.nodeify(cb);
	});
});

