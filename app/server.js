'use strict';
var Q = require('q');
var minimist = require('minimist');
var path = require('path');
var forever = require('forever');
var child_process = require('child_process');

var pomeloLogger = require('pomelo-logger');
var logger = pomeloLogger.getLogger('memorydb', __filename);

var startShard = function(opts){
	var Database = require('./database');
	var server = require('socket.io')();

	var db = new Database(opts);
	db.start().then(function(){
		logger.warn('server started');
	});

	server.on('connection', function(socket){
		var connId = db.connect();
		var remoteAddr = socket.conn.remoteAddress;

		socket.on('req', function(msg){
			logger.info('[%s] %s => %j', connId, remoteAddr, msg);
			var resp = {seq : msg.seq};

			Q.fcall(function(){
				var method = msg.method;
				var args = [connId].concat(msg.args);
				return db[method].apply(db, args);
			})
			.then(function(ret){
				resp.err = null;
				resp.data = ret;
			}, function(err){
				resp.err = err.stack;
				resp.data = null;
			})
			.then(function(){
				socket.emit('resp', resp);
				var level = resp.err ? 'warn' : 'info';
				logger[level]('[%s] %s <= %j', connId, remoteAddr, resp);
			})
			.catch(function(e){
				logger.error(e.stack);
			});
		});

		socket.on('disconnect', function(){
			db.disconnect(connId);
			logger.info('%s disconnected', remoteAddr);
		});

		logger.info('%s connected', remoteAddr);
	});

	server.listen(opts.port);

	var _isShutingDown = false;
	var shutdown = function(){
		if(_isShutingDown){
			return;
		}
		_isShutingDown = true;

		return Q.fcall(function(){
			server.close();

			return db.stop();
		})
		.catch(function(e){
			logger.error(e.stack);
		})
		.fin(function(){
			logger.warn('server closed');
			setTimeout(function(){
				process.exit(0);
			}, 200);
		});
	};

	process.on('SIGTERM', shutdown);
	process.on('SIGINT', shutdown);
};


if (require.main === module) {
	process.on('uncaughtException', function(err) {
		logger.error('Uncaught exception: %s', err.stack);
	});

	var argv = minimist(process.argv.slice(2));
	if(argv.help || argv.h){
		var usage = 'Usage: server.js [options]\n\n' +
					'Options:\n' +
					'  -c, --conf path 	Specify config file path (must with .json extension)\n' +
					'  -s, --shard shardId	Start specific shard\n' +
					'  -d, --daemon		Start as daemon\n' +
					'  -h, --help 		Display this help';
		console.log(usage);
		process.exit(0);
	}

	var searchPaths = [];
	var confPath = argv.conf || argv.c || null;
	if(confPath){
		searchPaths.push(confPath);
	}
	searchPaths.push(['./memorydb.json', '~/.memorydb.json', '/etc/memorydb.json']);
	var conf = null;
	for(var i=0; i<searchPaths.length; i++){
		try{
			conf = require(searchPaths[i]);
			break;
		}
		catch(e){
		}
	}
	if(!conf){
		console.error('Error: config file not found!');
		process.exit(1);
	}

	var shardId = argv.s || argv.shard || null;

	Q.longStackSupport = conf.q ? !!conf.q.longStackSupport : false;

	// Configure logger
	var loggerConf = conf.logger || {};

	var base = loggerConf.path || '/var/log/memorydb';
	pomeloLogger.configure(path.join(__dirname, 'log4js.json'), {shardId : shardId, base : base});

	var level = loggerConf.level || 'INFO';
	pomeloLogger.setGlobalLogLevel(pomeloLogger.levels[level]);

	var shards = conf.shards || {};

	var isDaemon = argv.d || argv.daemon;

	if(!shardId){
		// Main script, will start all shards via ssh
		Object.keys(shards).forEach(function(shardId){
			var host = shards[shardId].host;
			console.info('start %s via ssh...', shardId);
			// TODO: start shard
		});
	}
	else{
		// Start specific shard
		var shardConfig = shards[shardId];
		if(!shardConfig){
			console.error('config not found for shard %s', shardId);
			process.exit(1);
		}

		if(isDaemon && !argv.child){
			var args = process.argv.slice(2);
			args.push('--child');

			forever.startDaemon(__filename, {
				max : 1,
				slient : true,
				killTree : false,
				args : args,
			});
		}
		else{
			var opts = {
				_id : shardId,
				port : shardConfig.port,
				slaveConfig : shardConfig.slaveConfig,
				redisConfig : conf.redisConfig,
				backend : conf.backend,
				backendConfig : conf.backendConfig,
				collections : conf.collections,
			};
			startShard(opts);
		}
	}
}
