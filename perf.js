'use strict';

var memorydb = require('./lib');
var Q = require('q');
var _ = require('lodash');
var env = require('./test/env');
Q.longStackSupport = false;
var should = require('should');

var pomeloLogger = require('pomelo-logger');
pomeloLogger.setGlobalLogLevel(pomeloLogger.levels.WARN);

var logger = pomeloLogger.getLogger('test', __filename);

var writeSingleDoc = function(){
	var count = 5000;
	var autoconn = null;
	var player = {_id : 1, name : 'rain', exp : 0};

	var incPlayerExp = function(){
		return autoconn.execute(function(){
			var Player = autoconn.collection('player');
			return Q.fcall(function(){
				// player.exp = _.random(60);
				// return Player.update(player._id, player);
				return Player.findForUpdate(player._id);
			})
			.then(function(doc){
				doc.exp++;
				logger.debug('player.exp = %s', doc.exp);
				return Player.update(player._id, doc);
			});
		})
		.catch(function(e){
			logger.error(e);
		});
	};

	var startTick = null;
	var qps = null;
	return Q.fcall(function(){
		var config = env.dbConfig('s1');
		//config.disableSlave = true;
		return memorydb.start(config);
	})
	.then(function(){
		autoconn = memorydb.autoConnect();
		return autoconn.execute(function(){
			var Player = autoconn.collection('player');
			return Player.insert(player._id, player);
		});
	})
	.then(function(){
		startTick = Date.now();
		var promise = Q(); // jshint ignore:line
		for(var i=0; i<count; i++){
			promise = promise.then(incPlayerExp);
		}
		return promise;
	})
	.then(function(){
		qps = count * 1000 / (Date.now() - startTick);
		return autoconn.execute(function(){
			var Player = autoconn.collection('player');
			return Q.fcall(function(){
				return Player.find(player._id);
			})
			.then(function(ret){
				logger.warn('%j', ret);

				return Player.remove(player._id);
			});
		});
	})
	.fin(function(){
		return memorydb.stop()
		.fin(function(){
			logger.warn('QPS: %s', qps);
		});
	});
};

var writeHugeDoc = function(){

};

var crossShardsWrite = function(){

};

var main = function(){
	// var p = Q();
	// var start = Date.now();
	// var count = 100000;
	// for(var i=0; i<count; i++){
	// 	p = p.then(function(){
	// 	});
	// }
	// return p.then(function(){
	// 	var qps = count / (Date.now() - start) * 1000
	// 	logger.warn('qps %s', qps);
	// });

	return writeSingleDoc();
};

if (require.main === module) {
	return Q.fcall(function(){
		return Q.nfcall(function(cb){
			env.flushdb(cb);
		})
		.then(function(){
			return main();
		});
	})
	.fin(function(){
		process.exit();
	});
}
