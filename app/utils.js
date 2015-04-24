'use strict';

var exports = {};

// Make sure err is instanceof Error
exports.normalizecb = function(cb){
	return function(err, ret){
		if(!!err && !(err instanceof Error)){
			err = new Error(err);
		}
		cb(err, ret);
	};
};

exports.setPromiseConcurrency = function(P, concurrency){
	var map = P.prototype.map;
	P.prototype.map = function(fn, options){
		if(!options || !options.concurrency){
			options = {concurrency : concurrency};
		}
		return map.call(this, fn, options);
	};
};

exports.getObjPath = function(obj, path){
	var current = obj;
	path.split('.').forEach(function(field){
		if(!!current){
			current = current[field];
		}
	});
	return current;
};

exports.setObjPath = function(obj, path, value){
	if(typeof(obj) !== 'object'){
		throw new Error('not object');
	}
	var current = obj;
	var fields = path.split('.');
	var finalField = fields.pop();
	fields.forEach(function(field){
		if(!current.hasOwnProperty(field)){
			current[field] = {};
		}
		current = current[field];
		if(typeof(current) !== 'object'){
			throw new Error('path is exist and not a object');
		}
	});
	current[finalField] = value;
};

exports.deleteObjPath = function(obj, path){
	if(typeof(obj) !== 'object'){
		throw new Error('not object');
	}
	var current = obj;
	var fields = path.split('.');
	var finalField = fields.pop();
	fields.forEach(function(field){
		if(!!current){
			current = current[field];
		}
	});
	if(current !== undefined){
		delete current[finalField];
	}
};

module.exports = exports;
