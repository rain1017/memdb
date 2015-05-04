'use strict';

var path = require('path');
global.MONGOOSE_DRIVER_PATH = path.resolve(__dirname, './mongoose-driver/');
var P = require('bluebird');
var util = require('util');
var logger = require('pomelo-logger').getLogger('memdb-client', __filename);
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

// Parse mongoose model to generate collection config (indexes, uniques, etc)
mongoose.genCollectionConfig = function(){
	var collections = {};

	for(var name in mongoose.models){
		var model = mongoose.models[name];
		var schema = model.schema;
		var collname = model.collection.name;

		if(!collections[collname]){
			collections[collname] = {};
		}
		var collection = collections[collname];
		if(!collection.indexes){
			collection.indexes = [];
		}

		var paths = schema.paths;
		var index = null, mdbIndex = null;
		for(var field in paths){
			if(field === '_id' || field.indexOf('.') !== -1){
				continue; //ignore compound field and _id
			}
			index = paths[field]._index;
			if(!!index){
				mdbIndex = {keys : [field]};
				if(!!index.unique){
					mdbIndex.unique = true;
				}

				mdbIndex.valueIgnore = {};
				mdbIndex.valueIgnore[field] = paths[field].options.indexIgnore || [];
				collection.indexes.push(mdbIndex);
			}
		}

		if(!!schema._indexes){
			for(var i in schema._indexes){
				index = schema._indexes[i];
				mdbIndex = {keys : [], valueIgnore : {}};
				for(field in index[0]){
					if(index[0][field]){
						mdbIndex.keys.push(field);
						mdbIndex.valueIgnore[field] = paths[field].options.indexIgnore || [];
					}
				}
				if(index[1] && !!index[1].unique){
					mdbIndex.unique = true;
				}
				collection.indexes.push(mdbIndex);
			}
		}

		//Disable versionkey
		schema.options.versionKey = false;
	}

	logger.info('Parsed collection config : %j', collections);
	return collections;
};


var Model = mongoose.Model;

//Rename original 'findXXX' methds
Model.findMongo = Model.find;
Model.findOneMongo = Model.findOne;
Model.findByIdMongo = Model.findById;

Model.find = function(query, fields, opts) {
	var ModelCls = this;

	var promise = this.collection.findMdbAsync(query)
	.then(function(docs){
		return docs.map(function(doc){
			var m = new ModelCls();
			m.init(doc);
			m.__frommdb__ = true;
			return m;
		});
	});

	var cb = arguments[arguments.length - 1];
	if(typeof(cb) === 'function') {
		return promise.nodeify(cb);
	}
	else {
		return promise;
	}
};

Model.findOne = function(query) {
	var ModelCls = this;

	var promise = this.collection.findOneMdbAsync(query)
	.then(function(doc){
		if(!doc){
			return null;
		}
		var m = new ModelCls();
		m.init(doc);
		m.__frommdb__ = true;
		return m;
	});

	var cb = arguments[arguments.length - 1];
	if(typeof(cb) === 'function') {
		return promise.nodeify(cb);
	}
	else {
		return promise;
	}
};

Model.findById = function(id){
	var ModelCls = this;

	var promise = this.collection.findByIdMdbAsync(id)
	.then(function(doc){
		if(!doc){
			return null;
		}
		var m = new ModelCls();
		m.init(doc);
		m.__frommdb__ = true;
		return m;
	});

	var cb = arguments[arguments.length - 1];
	if(typeof(cb) === 'function') {
		return promise.nodeify(cb);
	}
	else {
		return promise;
	}
};

Model.findForUpdate = function(query) {
	var ModelCls = this;

	var promise = this.collection.findForUpdateAsync(query)
	.then(function(docs){
		return docs.map(function(doc){
			var m = new ModelCls();
			m.init(doc);
			m.__frommdb__ = true;
			return m;
		});
	});

	var cb = arguments[arguments.length - 1];
	if(typeof(cb) === 'function') {
		return promise.nodeify(cb);
	}
	else {
		return promise;
	}
};

Model.findOneForUpdate = function(query){
	var ModelCls = this;

	var promise = this.collection.findOneForUpdateAsync(query)
	.then(function(doc){
		if(!doc){
			return null;
		}
		var m = new ModelCls();
		m.init(doc);
		m.__frommdb__ = true;
		return m;
	});

	var cb = arguments[arguments.length - 1];
	if(typeof(cb) === 'function') {
		return promise.nodeify(cb);
	}
	else {
		return promise;
	}
};

Model.findByIdForUpdate = function(id){
	var ModelCls = this;

	var promise = this.collection.findByIdForUpdateAsync(id)
	.then(function(doc){
		if(!doc){
			return null;
		}
		var m = new ModelCls();
		m.init(doc);
		m.__frommdb__ = true;
		return m;
	});

	var cb = arguments[arguments.length - 1];
	if(typeof(cb) === 'function') {
		return promise.nodeify(cb);
	}
	else {
		return promise;
	}
};

Model.findByIdCached = function(id) {
	var ModelCls = this;

	var promise = this.collection.findByIdCachedAsync(id)
	.then(function(doc){
		if(!doc){
			return null;
		}
		var m = new ModelCls();
		m.init(doc);
		//m.__frommdb__ = true; //ReadOnly
		return m;
	});

	var cb = arguments[arguments.length - 1];
	if(typeof(cb) === 'function') {
		return promise.nodeify(cb);
	}
	else {
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
			if(typeof(cb) === 'function') {
				cb(new Error(util.format('not implemented method: ', funcName)));
			} else {
				throw new Error(util.format('not implemented method: ', funcName));
			}
		};
	})(overwrited[i]); //jshint ignore:line
}

module.exports = P.promisifyAll(mongoose);

