'use strict';

var Q = require('q');
var util = require('util');
var deepcopy = require('deepcopy');
var AsyncLock = require('async-lock');
var EventEmitter = require('events').EventEmitter;
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

var Document = function(opts){ //jshint ignore:line
	opts = opts || {};

	this.commited = opts.doc || {}; // {field : value}
	this.changed = {}; // {field : value}, value = undefined means field is removed
	this.commitedExist = opts.exist !== undefined ? !!opts.exist : true; // Whether this document is exist
	this.changedExist = undefined;

	this.connectionId = null; // Connection that hold the document lock
	this._lock = new AsyncLock({timeout : 1000000 * 1000}); // Never timeout
	this.releaseCallback = null;

	EventEmitter.call(this);
};

util.inherits(Document, EventEmitter);

var proto = Document.prototype;

/**
 * connectionId - current connection
 * fields - 'field1 field2'
 */
proto.find = function(connectionId, fields){
	var self = this;

	if(!self.exists(connectionId)){
		return null;
	}

	var doc = {};

	if(fields){
		fields.split(' ').forEach(function(field){
			// Lock holder should see the newest uncommited change
			if(self.isLocked(connectionId) && self.changed.hasOwnProperty(field)){
				if(self.changed[field] !== undefined){
					doc[field] = deepcopy(self.changed[field]);
				}
			}
			// Other connections should only see the commited value
			else if(self.commited.hasOwnProperty(field)){
				doc[field] = deepcopy(self.commited[field]);
			}
		});
		return doc;
	}
	else{
		// Lock holder should see the newest uncommited change
		if(self.isLocked(connectionId)){
			for(var field in self.commited){
				if(self.changed.hasOwnProperty(field)){
					// Modified field
					if(self.changed[field] !== undefined){
						doc[field] = deepcopy(self.changed[field]);
					}
				}
				else{
					doc[field] = deepcopy(self.commited[field]);
				}
			}
			for(field in self.changed){
				// Newly added field
				if(!self.commited.hasOwnProperty(field)){
					if(self.changed[field] !== undefined){
						doc[field] = deepcopy(self.changed[field]);
					}
				}
			}
		}
		// Other connections should only see the commited value
		else{
			doc = deepcopy(self.commited);
		}
		return doc;
	}
};

/**
 * opts.replace - replace the whole doc instead of update fields
 */
proto.update = function(connectionId, doc, opts){
	opts = opts || {};
	doc = doc || {};

	this.ensureLocked(connectionId);

	if(!this.exists(connectionId)){
		throw new Error('doc not exist');
	}

	if(opts.replace){
		this.changed = deepcopy(doc);

		// Replace the whole doc, set removed for absent field
		for(var field in this.commited){
			if(!this.changed.hasOwnProperty(field)){
				this.changed[field] = undefined;
			}
		}
	}
	else{
		for(var field in doc){ //jshint ignore:line
			this.changed[field] = deepcopy(doc[field]);
		}
	}
};

proto.insert = function(connectionId, doc){
	doc = doc || {};

	this.ensureLocked(connectionId);

	if(this.exists(connectionId)){
		throw new Error('doc already exists');
	}

	this.changedExist = true;
	this.update(connectionId, doc, {replace : true});
};

proto.remove = function(connectionId){
	this.ensureLocked(connectionId);

	if(!this.exists(connectionId)){
		throw new Error('doc not exist');
	}

	this.changedExist = false;
};

proto.exists = function(connectionId){
	if(connectionId !== this.connectionId){
		return this.commitedExist;
	}
	else{
		if(this.changedExist === undefined){
			return this.commitedExist;
		}
		else{
			return this.changedExist;
		}
	}
};

proto.lock = function(connectionId){
	var self = this;
	if(connectionId === null || connectionId === undefined){
		throw new Error('connectionId is null');
	}

	var deferred = Q.defer();
	if(self.isLocked(connectionId)){
		deferred.resolve();
	}
	else{
		var savedDomain = process.domain;
		self._lock.acquire('', function(release){
			process.domain = savedDomain;

			self.connectionId = connectionId;
			self.releaseCallback = release;
			self.changed = {};
			self.changedExist = undefined;

			deferred.resolve();
		});
	}
	return deferred.promise;
};

proto._unlock = function(){
	this.connectionId = null;

	var releaseCallback = this.releaseCallback;
	this.releaseCallback = null;
	process.nextTick(releaseCallback);
};

proto.commit = function(connectionId){
	this.ensureLocked(connectionId);

	if(this.changedExist !== undefined){
		this.commitedExist = this.changedExist;
	}

	if(this.commitedExist === false){
		this.commited = {};
	}
	else{
		for(var field in this.changed){
			if(this.changed[field] === undefined){
				delete this.commited[field];
			}
			else{
				this.commited[field] = this.changed[field];
			}
		}
	}

	this.changed = {};
	this.changedExist = undefined;

	this._unlock();
	this.emit('commit');
};

proto.rollback = function(connectionId){
	this.ensureLocked(connectionId);

	this.changed = {};
	this.changedExist = undefined;

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
