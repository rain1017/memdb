'use strict';

var Q = require('q');
var _ = require('lodash');
var should = require('should');
var assert = require('assert');
var Document = require('../lib/document'); // jshint ignore:line
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('document test', function(){
	it('find', function(){
		var value = {k1 : 1, k2 : 1};
		var doc = new Document({doc : value});
		// Get all fields
		doc.find('c1').should.eql(value);
		// Get specified fields
		doc.find('c1', 'k1').should.eql({k1 : 1});
	});

	it('update', function(cb){
		var value = {k1 : 1, k2 : 1};
		var doc = new Document({doc : value});

		doc.on('updateUncommited', function(field, oldValue, newValue){
			logger.debug(field, oldValue, newValue);
		});

		return Q.fcall(function(){
			// Lock for write
			return doc.lock('c1');
		})
		.then(function(){
			// Update a field
			doc.update('c1', {k1 : 2});
			doc.find('c1').should.eql({k1 : 2, k2 : 1});
			doc.find('c1', 'k1').should.eql({k1 : 2});
			// Remove a field
			doc.update('c1', {k2 : undefined});
			doc.find('c1').should.eql({k1 : 2});
			doc.find('c1', 'k2').should.eql({});
			// Add a field
			doc.update('c1', {k3 : 1});
			doc.find('c1').should.eql({k1 : 2, k3: 1});
			doc.find('c1', 'k3').should.eql({k3 : 1});
			// Replace the whole doc
			doc.update('c1', {k1 : 1}, {replace : true});
			doc.find('c1').should.eql({k1 : 1});
		})
		.nodeify(cb);
	});

	it('insert/remove', function(cb){
		var value = {k1 : 1};
		// Init with non-exist doc
		var doc = new Document({exist : false});

		doc.on('updateUncommited', function(field, oldValue, newValue){
			logger.debug(field, oldValue, newValue);
		});

		return Q.fcall(function(){
			assert(doc.find('c1') === null);
			// Lock for write
			return doc.lock('c1');
		})
		.then(function(){
			// insert doc
			doc.insert('c1', value);
			doc.find('c1').should.eql(value);
			// should throw exception when insert existing doc
			should(doc.insert.bind(doc, 'c1', value)).throw();
			// remove doc
			doc.remove('c1');
			assert(doc.find('c1') === null);
			// should throw exception when remove non-exist doc
			should(doc.remove.bind(doc, 'c1')).throw();
		})
		.nodeify(cb);
	});

	it('commit/rollback/lock/unlock', function(cb){
		var value = {k1 : 1};
		var doc = new Document({doc : value});

		doc.on('updateUncommited', function(field, oldValue, newValue){
			logger.debug(field, oldValue, newValue);
		});

		return Q.fcall(function(){
			// should throw when write without lock
			should(doc.remove.bind(doc, 'c1')).throw();

			return doc.lock('c1')
			.then(function(){
				// lock twice should be ok
				return doc.lock('c1');
			});
		})
		.then(function(){
			doc.update('c1', {k1 : 2});
			doc.find('c1').should.eql({k1 : 2});
			doc.rollback('c1');
			// should rolled back
			doc.find('c1').should.eql({k1 : 1});
			// should unlocked
			should(doc.remove.bind(doc, 'c1')).throw();

			return doc.lock('c1');
		})
		.then(function(){
			doc.update('c1', {k1 : 2});
			doc.commit('c1');
			// should commited
			doc.find('c1').should.eql({k1 : 2});
			// should unlocked
			should(doc.remove.bind(doc, 'c1')).throw();

			// remove and rollback
			return doc.lock('c1');
		})
		.then(function(){
			doc.remove('c1');
			doc.rollback('c1');
			assert(doc.find('c1') !== null);

			// remove and commit
			return doc.lock('c1');
		})
		.then(function(){
			doc.remove('c1');
			doc.commit('c1');
			assert(doc.find('c1') === null);

			// insert and rollback
			return doc.lock('c1');
		})
		.then(function(){
			doc.insert('c1', value);
			doc.rollback('c1');
			assert(doc.find('c1') === null);

			// insert and commit
			return doc.lock('c1');
		})
		.then(function(){
			doc.insert('c1', value);
			doc.commit('c1');
			doc.find('c1').should.eql(value);
		})
		.nodeify(cb);
	});

	it('read from other connections', function(cb){
		var value = {k1 : 1, k2 : 1};
		var doc = new Document({doc : value});

		doc.on('updateUncommited', function(field, oldValue, newValue){
			logger.debug(field, oldValue, newValue);
		});

		return Q.fcall(function(){
			return doc.lock('c1');
		})
		.then(function(){
			// update field
			doc.update('c1', {k1 : 2});
			// read from c2
			doc.find('c2', 'k1').should.eql({k1 : 1});
			doc.find('c2').should.eql(value);
			// add field
			doc.update('c1', {k3 : 1});
			doc.find('c2').should.eql(value);
			// remove field
			doc.update('c1', {k2 : undefined});
			doc.find('c2').should.eql(value);
			// replace doc
			doc.update('c1', {k1 : 1}, {replace : true});
			doc.find('c2').should.eql(value);
			// commit
			doc.commit('c1');
			// c2 should see the newest value now
			doc.find('c2').should.eql({k1 : 1});

			// remove and commit
			return doc.lock('c1');
		})
		.then(function(){
			doc.remove('c1');
			assert(doc.find('c2') !== null);
			doc.commit('c1');
			assert(doc.find('c2') === null);

			// insert and commit
			return doc.lock('c1');
		})
		.then(function(){
			doc.insert('c1', value);
			assert(doc.find('c2') === null);
			doc.commit('c1');
			doc.find('c2').should.eql(value);
		})
		.nodeify(cb);
	});

	it('concurrency', function(cb){
		var doc = new Document({doc : {k : 0}});

		doc.on('updateUncommited', function(field, oldValue, newValue){
			logger.debug(field, oldValue, newValue);
		});

		var concurrency = 4;
		// Simulate non-atomic check and update
		return Q.all(_.range(concurrency).map(function(connId){
			var value = null;
			return Q() //jshint ignore:line
			.delay(_.random(10))
			.then(function(){
				logger.trace('%s start lock', connId);
				return doc.lock(connId);
			})
			.then(function(){
				logger.trace('%s got lock', connId);
				value = doc.find(connId);
			})
			.delay(_.random(20))
			.then(function(){
				value.k++;
				doc.update(connId, value);
				doc.commit(connId);
				logger.trace('%s commited', connId);
			});
		}))
		.then(function(){
			//Result should equal to concurrency
			doc.find(null).should.eql({k : concurrency});
		})
		.nodeify(cb);
	});
});
