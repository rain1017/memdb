'use strict';

var P = require('bluebird');
var util = require('util');
var utils = require('./utils');
var clone = require('clone');
var AsyncLock = require('async-lock');
var EventEmitter = require('events').EventEmitter;
var modifier = require('./modifier');
var logger = require('pomelo-logger').getLogger('memdb', __filename);

/**
 *
 * opts.exist
 * opts.doc
 *
 * Events:
 *
 * updateUncommited - (connectionId, field, oldValue, newValue)
 * Used for internal index update (won't fire on commit or rollback)
 */
var Document = function(opts){ //jshint ignore:line
	opts = opts || {};

	var doc = opts.doc || null;
	if(typeof(doc) !== 'object'){
		throw new Error('doc must be object');
	}

	this.commited = doc;
	this.changed = undefined; // undefined means no change (while null means removed)

	this.watchedFields = opts.watchedFields || []; // fire event on change

	this.connectionId = null; // Connection that hold the document lock
	this._lock = new AsyncLock({
								Promise : P,
								timeout : 1000000 * 1000, // Never timeout
								});
	this.releaseCallback = null;

	EventEmitter.call(this);
};

util.inherits(Document, EventEmitter);

var proto = Document.prototype;

/**
 * connectionId - current connection
 */
proto.find = function(connectionId, fields){
	var doc = this.isLocked(connectionId) ? this.changed : this.commited;

	if(doc === null){
		return null;
	}

	if(!fields){
		return clone(doc);
	}

	var includeFields = [], excludeFields = [];

	if(typeof(fields) === 'string'){
		includeFields = fields.split(' ');
	}
	else if(typeof(fields) === 'object'){
		for(var field in fields){
			if(!!fields[field]){
				includeFields.push(field);
			}
			else{
				excludeFields.push(field);
			}
		}
		if(includeFields.length > 0 && excludeFields.length > 0){
			throw new Error('Can not specify both include and exclude fields');
		}
	}

	var ret = {};
	if(includeFields.length > 0){
		includeFields.forEach(function(field){
			if(doc.hasOwnProperty(field)){
				ret[field] = clone(doc[field]);
			}
		});
	}
	else if(excludeFields.length > 0){
		ret = clone(doc);
		excludeFields.forEach(function(field){
			delete ret[field];
		});
	}

	return ret;
};

proto.exists = function(connectionId){
	return this.isLocked(connectionId) ? this.changed !== null: this.commited !== null;
};

proto.insert = function(connectionId, doc){
	this.modify(connectionId, '$insert',  doc);
};

proto.remove = function(connectionId){
	this.modify(connectionId, '$remove');
};

proto.update = function(connectionId, doc, opts){
	opts = opts || {};
	doc = doc || {};

	if(this.changed === null && opts.upsert){
		this.modify(connectionId, '$insert', {});
	}

	var isModify = false;
	for(var field in doc){
		if(field.slice(0, 1) === '$'){
			isModify = true;
			break;
		}
	}

	if(!isModify){
		this.modify(connectionId, '$replace', doc);
	}
	else{
		for(var cmd in doc){
			this.modify(connectionId, cmd, doc[cmd]);
		}
	}
};

proto.modify = function(connectionId, cmd, param){
	this.ensureLocked(connectionId);

	var oldValues = {};
	var self = this;
	this.watchedFields.forEach(function(field){
		oldValues[field] = self.changed ? JSON.stringify(self.changed[field]) : undefined;
	});

	var modifyFunc = modifier[cmd];
	if(typeof(modifyFunc) !== 'function'){
		throw new Error('invalid modifier - ' + cmd);
	}

	this.changed = modifyFunc(this.changed, param);

	this.watchedFields.forEach(function(field){
		var value = self.changed ? JSON.stringify(self.changed[field]) : undefined;
		if(oldValues[field] !== value){
			self.emit('updateUncommited', connectionId, field, oldValues[field], value);
		}
	});
};

proto.lock = function(connectionId){
	var self = this;
	if(connectionId === null || connectionId === undefined){
		throw new Error('connectionId is null');
	}

	var deferred = P.defer();
	if(self.isLocked(connectionId)){
		deferred.resolve();
	}
	else{
		self._lock.acquire('', function(release){
			self.connectionId = connectionId;
			self.releaseCallback = release;
			self.changed = clone(self.commited);

			deferred.resolve();
		});
	}
	return deferred.promise;
};

proto._unlock = function(){
	if(this.connectionId === null){
		return;
	}
	this.connectionId = null;
	var releaseCallback = this.releaseCallback;
	this.releaseCallback = null;
	process.nextTick(releaseCallback);
};

proto._getChanged = function(){
	return this.changed !== undefined ? this.changed : this.commited;
};

proto.commit = function(connectionId){
	this.ensureLocked(connectionId);

	this.commited = this.changed;
	this.changed = undefined;

	this._unlock();
	this.emit('commit');
};

proto.rollback = function(connectionId){
	this.ensureLocked(connectionId);

	this.changed = undefined;

	this._unlock();
	this.emit('rollback');
};

proto.ensureLocked = function(connectionId){
	if(!this.isLocked(connectionId)){
		throw new Error('doc not locked by ' + connectionId);
	}
};

proto.isLocked = function(connectionId){
	return this.connectionId === connectionId && connectionId !== null && connectionId !== undefined;
};

module.exports = Document;
