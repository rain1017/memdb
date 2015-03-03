'use strict';

var path = require('path');
global.MONGOOSE_DRIVER_PATH = path.resolve(__dirname, './mongoose-driver/');

var mongoose = require('mongoose');
var Q = require('q');
var util = require('util');

module.exports = mongoose;

mongoose.Model.findOneInMdb = function(id, cb) {
	var ModelCls = this;
	var promise = Q.ninvoke(this.collection, 'findOneInMdb', id)
	.then(function(doc){
		var m = new ModelCls();
		m.init(doc);
		m.__frommdb__ = true;
		return m;
	});
	if(cb) {
		return promise.nodeify(cb);
	} else {
		return promise;
	}
};

var overwrited = ['save', 'remove', 'update'];

for (var i = overwrited.length - 1; i >= 0; i--) {
	(function(funcName){
		var oldFunc = mongoose.Model.prototype[funcName];
		mongoose.Model.prototype[funcName] = function (fn) {
			if(!this.__frommdb__ && !this.isNew) {
				return fn(new Error(util.format('can not %s a document which is not from memorydb.', funcName)));
			}
			oldFunc.apply(this, arguments);
		};
	})(overwrited[i]); //jshint ignore:line
}

var overwrited = ['findByIdAndRemove', 'findByIdAndUpdate', 'findOneAndUpdate', 'findOneAndRemove'];

for (var i = overwrited.length - 1; i >= 0; i--) {
	(function(funcName){
		mongoose.Model[funcName] = function(){
			var cb = arguments[arguments.length-1];
			if(cb && typeof cb === 'function') {
				cb(new Error(util.format('not implemented method: ', funcName)));
			} else {
				throw new Error(util.format('not implemented method: ', funcName));
			}
		};
	})(overwrited[i]); //jshint ignore:line
}

