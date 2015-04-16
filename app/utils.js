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

module.exports = exports;
