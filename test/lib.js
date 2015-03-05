'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var env = require('./env');
var memorydb = require('../lib');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('lib test', function(){
	beforeEach(function(cb){
		env.flushdb(cb);
	});
	after(function(cb){
		env.flushdb(cb);
	});

	it('lib test', function(cb){
		var user1 = {_id : 1, name : 'rain', level : 0};

		return Q.fcall(function(){
			return memorydb.start(env.dbConfig('s1'));
		})
		.then(function(){
			var autoconn = memorydb.autoConnect();
			return autoconn.execute(function(){
				var User = autoconn.collection('user');
				return User.insert(user1._id, user1);
			})
			.fin(function(){
				autoconn.close();
			});
		})
		.then(function(){
			var conn = memorydb.connect();
			return conn.collection('user').find(user1._id)
			.then(function(ret){
				ret.should.eql(user1);
			})
			.then(function(ret){
				return conn.collection('user').remove(user1._id);
			})
			.fin(function(){
				conn.commit();
				conn.close();
			});
		})
		.fin(function(){
			return memorydb.stop();
		})
		.nodeify(cb);
	});
});
