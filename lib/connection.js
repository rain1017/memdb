'use strict';

var uuid = require('node-uuid');

// var DEFAULT_CONNECTION_TIMEOUT = 180 * 1000;

var Connection = function(opts){
	opts = opts || {};
	this._id = opts._id || uuid.v4();
	this.lockedKeys = {};

	// this.connectionTimeoutValue = opts.connectionTimeout || DEFAULT_CONNECTION_TIMEOUT;
	// this.connectionTimeout = null;
};

var proto = Connection.prototype;

proto.isDirty = function() {
	return !!Object.keys(this.lockedKeys).length;
};

proto.addLockedKey = function(key){
	this.lockedKeys[key] = true;
};

proto.clearLockedKeys = function(){
	this.lockedKeys = {};
};

proto.getLockedKeys = function(){
	return Object.keys(this.lockedKeys);
};

module.exports = Connection;
