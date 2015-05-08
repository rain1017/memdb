'use strict';

var utils = require('./utils');

var Connection = function(opts){
	opts = opts || {};
	this._id = opts._id || utils.uuid();
	this.isBusy = false;
	this.lockedKeys = {};
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
