'use strict';

var Q = require('q');
var util = require('util');
var deepcopy = require('deepcopy');
var AsyncLock = require('async-lock');
var EventEmitter = require('events').EventEmitter;
var logger = require('pomelo-logger').getLogger('memorydb', __filename);

/**
 *
 *
 * Events:
 *
 * updateUncommited - (connectionId, field, oldValue, newValue)
 * Used for internal index update (won't fire on commit or rollback)
 *
 */
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
 * doc - {key : value}, value = undefined means remove the key
 * opts.replace - replace the whole doc instead of update fields
 * opts.upsert - insert if not exist
 */
proto.update = function(connectionId, doc, opts){
	opts = opts || {};
	doc = doc || {};

	this.ensureLocked(connectionId);

	if(!this.exists(connectionId)){
		if(opts.upsert){
			return this.insert(connectionId, doc);
		}
		else{
			throw new Error('doc not exist');
		}
	}

	var field = null, oldValue = null, newValue = null;

	if(opts.replace){
		var oldChanged = this.changed;
		this.changed = deepcopy(doc);

		for(field in doc){
			oldValue = oldChanged.hasOwnProperty(field) ? oldChanged[field] : this.commited[field];
			newValue = this.changed[field];
			if(oldValue !== newValue){
				this.emit('updateUncommited', connectionId, field, oldValue, newValue);
			}
		}

		// Replace the whole doc, set removed for absent field
		for(field in this.commited){
			if(!this.changed.hasOwnProperty(field)){
				oldValue = this.commited[field];

				this.changed[field] = undefined;

				newValue = this.changed[field];
				if(oldValue !== newValue){
					this.emit('updateUncommited', connectionId, field, oldValue, newValue);
				}
			}
		}
	}
	else{
		for(field in doc){ //jshint ignore:line
			oldValue = this.changed.hasOwnProperty(field) ? this.changed[field] : this.commited[field];

			this.changed[field] = deepcopy(doc[field]);

			newValue = this.changed[field];
			if(oldValue !== newValue){
				this.emit('updateUncommited', connectionId, field, oldValue, newValue);
			}
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

	var field = null, oldValue = null;

	for(field in this.commited){
		oldValue = this.changed.hasOwnProperty(field) ? this.changed[field] : this.commited[field];
		if(oldValue !== undefined){
			this.emit('updateUncommited', connectionId, field, oldValue, undefined);
		}
	}
	for(field in this.changed){
		if(this.commited.hasOwnProperty(field)){
			continue;
		}
		oldValue = this.changed[field];
		if(oldValue !== undefined){
			this.emit('updateUncommited', connectionId, field, oldValue, undefined);
		}
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

proto.getChange = function(connectionId){
	this.ensureLocked(connectionId);
	return {fields : this.changed, exist : this.changedExist};
};

module.exports = Document;
