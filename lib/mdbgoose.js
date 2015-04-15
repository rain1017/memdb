'use strict';

var path = require('path');
global.MONGOOSE_DRIVER_PATH = path.resolve(__dirname, './mongoose-driver/');
var P = require('bluebird');
var util = require('util');
var memdb = require('./index');

var mongoose = require('mongoose');

mongoose.setConnectOpts = function(opts){
	this.mdbConnectOpts = opts || {};
};

mongoose.autoConnect = function(cb){
	return memdb.autoConnect(this.mdbConnectOpts);
};

mongoose.execute = function(func){
	var self = this;
	return P.try(function(){
		return self.autoConnect();
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

	var promise = this.collection.findMdbAsync(id)
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
	var promise = this.collection.findForUpdateAsync(id)
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
	var promise = this.collection.findByIndexAsync(field, value)
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
	var promise = this.collection.findCachedAsync(id)
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
				return fn(new Error(util.format('can not %s a document which is not from memdb.', funcName)));
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

module.exports = P.promisifyAll(mongoose);

