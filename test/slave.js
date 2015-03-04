'use strict';

var Q = require('q');
var util = require('util');
var should = require('should');
var env = require('./env');
var Slave = require('../lib/slave');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('slave test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('insert/remove', function(cb){
		var shard = {_id : 's1'};
		var slave = new Slave(shard, env.redisConfig);

		var key1 = 'player:1';
		var doc1 = {exist : true, fields : {name : 'rain', age : 30}};
		var key2 = 'player:2';
		var doc2 = {exist : false, fields : {}};
		var changes = {
			'player:1' : {
				exist : undefined,
				fields : {name : 'snow', age : undefined}
			},
			'player:2' : {
				exist : true,
				fields : {name : 'tina'},
			},
		};

		return Q.fcall(function(){
			return slave.start();
		})
		.then(function(){
			return slave.insert(key1, doc1.fields, doc1.exist);
		})
		.then(function(){
			return slave.insert(key2, doc2.fields, doc2.exist);
		})
		.then(function(){
			return slave.findMulti([key1, key2])
			.then(function(docs){
				logger.debug(util.inspect(docs));
				docs[key1].should.eql(doc1);
				docs[key2].should.eql(doc2);
			});
		})
		.then(function(){
			return slave.commit(changes);
		})
		.then(function(){
			return slave.findMulti([key1, key2])
			.then(function(docs){
				logger.debug(util.inspect(docs));
				docs[key1].should.eql({
					exist : true,
					fields : {name : 'snow'},
				});
				docs[key2].should.eql({
					exist : true,
					fields : {name : 'tina'},
				});
			});
		})
		.then(function(){
			return slave.remove(key2);
		})
		.then(function(){
			return slave.remove(key1);
		})
		.then(function(){
			return slave.findMulti([key1, key2])
			.then(function(docs){
				logger.debug(util.inspect(docs));
				(docs[key1] === null).should.eql(true);
				(docs[key2] === null).should.eql(true);
			});
		})
		.then(function(){
			return slave.stop();
		})
		.nodeify(cb);
	});

	it('getAll/clear', function(cb){
		var shard = {_id : 's1'};
		var slave = new Slave(shard, env.redisConfig);

		var key1 = 'player:1';
		var doc1 = {exist : true, fields : {name : 'rain', age : 30}};
		var key2 = 'player:2';
		var doc2 = {exist : false, fields : {}};

		return Q.fcall(function(){
			return slave.start();
		})
		.then(function(){
			return slave.insert(key1, doc1.fields, doc1.exist);
		})
		.then(function(){
			return slave.insert(key2, doc2.fields, doc2.exist);
		})
		.then(function(){
			return slave.getAllKeys()
			.then(function(keys){
				logger.debug(util.inspect(keys));
				keys.length.should.eql(2);
			});
		})
		.then(function(){
			return slave.clear();
		})
		.then(function(){
			return slave.getAllKeys()
			.then(function(keys){
				logger.debug(util.inspect(keys));
				keys.length.should.eql(0);
			});
		})
		.nodeify(cb);
	});
});
