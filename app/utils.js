'use strict';

var _ = require('lodash');

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

exports.clone = function(obj){
	return JSON.parse(JSON.stringify(obj));
};

exports.cloneEx = function(obj){
	if(obj === null || obj === undefined || typeof(obj) === 'number' || typeof(obj) === 'string' || typeof(obj) === 'boolean'){
		return obj;
	}
	if(typeof(obj) === 'object'){
		var copy = Array.isArray(obj) ? new Array(obj.length) : {};
		for(var key in obj){
			copy[key] = exports.clone(obj[key]);
		}
		return copy;
	}
	throw new Error('unsupported type of obj: ' + obj);
};

exports.uuid = function(){
	// return a short uuid, based on current tick and a random number
	// result uuid like '2mnh1r3wb'
	return ((Date.now() - 1422720000000) * 1000 + _.random(1000)).toString(36);
};

module.exports = exports;
