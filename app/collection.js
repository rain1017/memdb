'use strict';

var P = require('bluebird');
var util = require('util');
var utils = require('./utils');
var EventEmitter = require('events').EventEmitter;
var logger = require('pomelo-logger').getLogger('memdb', __filename);

var MAX_INDEX_COLLISION = 10000;

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

	this.shard.on('docUpdateIndex:' + this.name, function(connId, id, field, oldValue, newValue){
		if(!!self.indexes[field]){
			if(!self.pendingIndexTasks[id]){
				self.pendingIndexTasks[id] = [];
			}
			if(oldValue !== null){
				self.pendingIndexTasks[id].push(self._removeIndex(connId, id, field, oldValue));
			}
			if(newValue !== null){
				self.pendingIndexTasks[id].push(self._insertIndex(connId, id, field, newValue));
			}
		}
	});

	EventEmitter.call(this);
};

util.inherits(Collection, EventEmitter);

var proto = Collection.prototype;

proto.insert = function(connId, id, doc){
	if(id === null || id === undefined){
		id = utils.uuid();
	}
	id = this._checkId(id);

	return P.bind(this)
	.then(function(){
		return this.lock(connId, id);
	})
	.then(function(){
		return this.shard.insert(connId, this._key(id), doc);
	})
	.then(function(){
		return this._finishIndexTasks(id);
	})
	.then(function(){
		return id;
	});
};

proto.remove = function(connId, id){
	id = this._checkId(id);

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
	id = this._checkId(id);

	return this.shard.find(connId, this._key(id), fields);
};

proto.findForUpdate = function(connId, id, fields){
	id = this._checkId(id);

	return P.bind(this)
	.then(function(){
		return this.lock(connId, id);
	})
	.then(function(){
		return this.find(connId, id, fields);
	});
};

proto.update = function(connId, id, doc, opts){
	id = this._checkId(id);

	var self = this;
	return P.try(function(){
		return self.lock(connId, id);
	})
	.then(function(){
		return self.shard.update(connId, self._key(id), doc, opts);
	})
	.then(function(){
		return self._finishIndexTasks(id);
	});
};

proto.lock = function(connId, id){
	id = this._checkId(id);

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

// value is object
proto.findByIndex = function(connId, field, value, fields){
	if(typeof(field) !== 'string'){
		throw new Error('field must be string');
	}
	if(value === null || value === undefined){
		throw new Error('value can not be null or undefined');
	}

	var indexCollection = this.db._collection(this._indexCollectionName(field));
	return P.bind(this)
	.then(function(){
		return indexCollection.find(connId, JSON.stringify(value));
	})
	.then(function(doc){
		var ids = doc ? Object.keys(doc.ids) : [];
		var self = this;
		return P.map(ids, function(id64){
			var id = new Buffer(id64, 'base64').toString();
			return self.find(connId, id, fields);
		});
	});
};

proto.findCached = function(connId, id){
	id = this._checkId(id);

	return this.shard.findCached(connId, this._key(id));
};

// value is json encoded
proto._insertIndex = function(connId, id, field, value){
	var indexCollection = this.db._collection(this._indexCollectionName(field));
	var param = {
		$set : {},
		$inc : {count : 1},
	};
	var id64 = new Buffer(id).toString('base64');
	param.$set['ids.' + id64] = 1;

	logger.warn('insertIndex %j', param);

	return P.try(function(){
		return indexCollection.find(connId, value, 'count');
	})
	.then(function(ret){
		if(!!ret && ret.count >= MAX_INDEX_COLLISION){
			throw new Error('Too many duplicate values on same index');
		}
		return indexCollection.update(connId, value, param, {upsert : true});
	});
};

// value is json encoded
proto._removeIndex = function(connId, id, field, value){
	var indexCollection = this.db._collection(this._indexCollectionName(field));

	return P.try(function(){
		var param = {
			$unset : {},
			$inc: {count : -1}
		};
		var id64 = new Buffer(id).toString('base64');
		param.$unset['ids.' + id64] = 1;
		return indexCollection.update(connId, value, param);
	})
	.then(function(){
		return indexCollection.find(connId, value, 'count');
	})
	.then(function(ret){
		if(ret.count === 0){
			return indexCollection.remove(connId, value);
		}
	});
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
	return '__i_' + this.name + '_' + field;
};

proto._key = function(id){
	return this.name + ':' + id;
};

proto._checkId = function(id){
	if(typeof(id) === 'number'){
		return id.toString();
	}
	else if(typeof(id) === 'string'){
		return id;
	}
	throw new Error('id must be number or string');
};

module.exports = Collection;
