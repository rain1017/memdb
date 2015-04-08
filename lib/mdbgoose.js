'use strict';

var path = require('path');
global.MONGOOSE_DRIVER_PATH = path.resolve(__dirname, './mongoose-driver/');
var Q = require('q');
var util = require('util');
var memorydb = require('./index');

var mongoose = require('mongoose');

mongoose.connectMdb = function(host, port){
	global.MONGOOSE_MDB_HOST = host;
	global.MONGOOSE_MDB_PORT = port;
};

mongoose.execute = function(func){
	return Q.fcall(function(){
		return memorydb.autoConnect(global.MONGOOSE_MDB_HOST, global.MONGOOSE_MDB_PORT);
	})
	.then(function(conn){
		return conn.execute(func);
	});
};

var Model = mongoose.Model;

//Rename original 'find'
Model.findMongo = Model.find;

Model.find = function(id, cb) {
	var ModelCls = this;
	var promise = Q.ninvoke(this.collection, 'findMdb', id)
	.then(function(doc){
		if(doc === null){
			return;
		}
		var m = new ModelCls();
		m.init(doc);
		m.__frommdb__ = true;
		return m;
	});
	if(cb) {
		return promise.nodeify(cb);
	} else {
		return promise;
	}
};

Model.findForUpdate = function(id, cb) {
	var ModelCls = this;
	var promise = Q.ninvoke(this.collection, 'findForUpdate', id)
	.then(function(doc){
		if(doc === null){
			return;
		}
		var m = new ModelCls();
		m.init(doc);
		m.__frommdb__ = true;
		return m;
	});
	if(cb) {
		return promise.nodeify(cb);
	} else {
		return promise;
	}
};

Model.findByIndex = function(field, value, cb) {
	var ModelCls = this;
	var promise = Q.ninvoke(this.collection, 'findByIndex', field, value)
	.then(function(docs){
		return docs.map(function(doc){
			if(doc === null){
				return;
			}
			var m = new ModelCls();
			m.init(doc);
			m.__frommdb__ = true;
			return m;
		});
	});
	if(cb) {
		return promise.nodeify(cb);
	} else {
		return promise;
	}
};

Model.findCached = function(id, cb) {
	var ModelCls = this;
	var promise = Q.ninvoke(this.collection, 'findCached', id)
	.then(function(doc){
		if(doc === null){
			return;
		}
		var m = new ModelCls();
		m.init(doc);
		//m.__frommdb__ = true; Readonly
		return m;
	});
	if(cb) {
		return promise.nodeify(cb);
	} else {
		return promise;
	}
};

var overwrited = ['save', 'remove', 'update'];

for (var i = overwrited.length - 1; i >= 0; i--) {
	(function(funcName){
		var oldFunc = Model.prototype[funcName];
		Model.prototype[funcName] = function (fn) {
			if(!this.__frommdb__ && !this.isNew) {
				return fn(new Error(util.format('can not %s a document which is not from memorydb.', funcName)));
			}
			oldFunc.apply(this, arguments);
		};
	})(overwrited[i]); //jshint ignore:line
}

var overwrited = ['findByIdAndRemove', 'findByIdAndUpdate', 'findOneAndUpdate', 'findOneAndRemove'];

for (var i = overwrited.length - 1; i >= 0; i--) {
	(function(funcName){
		mongoose.Model[funcName] = function(){
			var cb = arguments[arguments.length-1];
			if(cb && typeof cb === 'function') {
				cb(new Error(util.format('not implemented method: ', funcName)));
			} else {
				throw new Error(util.format('not implemented method: ', funcName));
			}
		};
	})(overwrited[i]); //jshint ignore:line
}


// Add Q support for mongoose
var funcNames = {
	MODEL_STATICS: [
		// mongoose.Model static
		'remove', 'ensureIndexes', 'find', 'findById', 'findOne', 'count', 'distinct',
		'findOneAndUpdate', 'findByIdAndUpdate', 'findOneAndRemove', 'findByIdAndRemove',
		'create', 'update', 'mapReduce', 'aggregate', 'populate',
		'geoNear', 'geoSearch',
		// added by memorydb
		'findMongo', 'findForUpdate', 'findByIndex', 'findCached',
		// mongoose.Document static
		'update'
	],
	MODEL_METHODS: [
		// mongoose.Model instance
		'save', 'remove',
		// mongoose.Document instance
		'populate', 'update', 'validate'
	],
	QUERY_METHODS: [
		// mongoose.Query instance
		'find', 'exec', 'findOne', 'count', 'distinct', 'update', 'remove',
		'findOneAndUpdate', 'findOneAndRemove', 'lean', 'limit', 'skip', 'sort'
	],
	AGGREGATE_METHODS: [
		'exec'
	]
};

module.exports = require('mongoose-q')(mongoose, {funcNames : funcNames});
