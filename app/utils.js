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

module.exports = exports;
