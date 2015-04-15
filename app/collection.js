'use strict';

var P = require('bluebird');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var logger = require('pomelo-logger').getLogger('memdb', __filename);

/**
 * opts.indexes - [field, field]
 *
 */
var Collection = function(opts){
	var self = this;

	this.name = opts.name;
	this.shard = opts.shard;
	this.db = opts.db;
	this.config = opts.config || {};

	this.indexes = {}; //{field : true}
	(this.config.indexes || []).forEach(function(index){
		self.indexes[index] = true;
	});

	this.pendingIndexTasks = {}; //{id, [Promise]}

	this.shard.on('docUpdateUncommited:' + this.name, function(connId, id, field, oldValue, newValue){
		if(!!self.indexes[field]){
			if(!self.pendingIndexTasks[id]){
				self.pendingIndexTasks[id] = [];
			}
			if(oldValue !== undefined){
				self.pendingIndexTasks[id].push(self._removeIndex(connId, id, field, oldValue));
			}
			if(newValue !== undefined){
				self.pendingIndexTasks[id].push(self._insertIndex(connId, id, field, newValue));
			}
		}
	});

	EventEmitter.call(this);
};

util.inherits(Collection, EventEmitter);

var proto = Collection.prototype;

proto.insert = function(connId, id, doc){
	return P.bind(this)
	.then(function(){
		return this.lock(connId, id);
	})
	.then(function(){
		return this.shard.insert(connId, this._key(id), doc);
	})
	.then(function(){
		return this._finishIndexTasks(id);
	});
};

proto.remove = function(connId, id){
	return P.bind(this)
	.then(function(){
		return this.lock(connId, id);
	})
	.then(function(){
		return this.shard.remove(connId, this._key(id));
	})
	.then(function(){
		return this._finishIndexTasks(id);
	});
};

proto.find = function(connId, id, fields){
	return this.shard.find(connId, this._key(id), fields);
};

proto.findForUpdate = function(connId, id, fields){
	return P.bind(this)
	.then(function(){
		return this.lock(connId, id);
	})
	.then(function(){
		return this.find(connId, id, fields);
	});
};

proto.update = function(connId, id, doc, opts){
	return P.bind(this)
	.then(function(){
		return this.lock(connId, id);
	})
	.then(function(){
		return this.shard.update(connId, this._key(id), doc, opts);
	})
	.then(function(){
		return this._finishIndexTasks(id);
	});
};

proto.lock = function(connId, id){
	if(this.shard.isLocked(connId, this._key(id))){
		return;
	}
	return P.bind(this)
	.then(function(){
		return this.shard.lock(connId, this._key(id));
	})
	.then(function(ret){
		this.emit('lock', connId, id);
		return ret;
	});
};

proto.findByIndex = function(connId, field, value, fields){
	var indexCollection = this.db._collection(this._indexCollectionName(field));
	return P.bind(this)
	.then(function(){
		return indexCollection.find(connId, value);
	})
	.then(function(ids){
		if(!ids){
			ids = {};
		}
		//TODO: this is a bug
		delete ids._id;

		return P.bind(this)
		.then(function(){
			return Object.keys(ids);
		})
		.map(function(id){
			return this.find(connId, id, fields);
		});
	});
};

proto.findCached = function(connId, id){
	return this.shard.findCached(connId, this._key(id));
};

proto._insertIndex = function(connId, id, field, value){
	if(id === '_id'){
		//TODO: this is a bug
		throw new Error('index key "_id" not supported');
	}

	var indexCollection = this.db._collection(this._indexCollectionName(field));
	var doc = {};
	doc[id] = true;
	return indexCollection.update(connId, value, doc, {upsert : true});
};

proto._removeIndex = function(connId, id, field, value){
	if(id === '_id'){
		//TODO: this is a bug
		throw new Error('index key "_id" not supported');
	}

	var indexCollection = this.db._collection(this._indexCollectionName(field));
	//TODO: Remove doc which is {}
	var doc = {};
	doc[id] = undefined;
	return indexCollection.update(connId, value, doc);
};

proto._finishIndexTasks = function(id){
	if(!this.pendingIndexTasks[id]){
		return;
	}
	return P.bind(this)
	.then(function(){
		return this.pendingIndexTasks[id];
	})
	.all()
	.then(function(){
		delete this.pendingIndexTasks[id];
	});
};

proto._indexCollectionName = function(field){
	return '__index_' + this.name + '_' + field;
};

proto._key = function(id){
	return this.name + ':' + id;
};

module.exports = Collection;
