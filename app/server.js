'use strict';

var P = require('bluebird');
var minimist = require('minimist');
var path = require('path');
var fs = require('fs');
var memdbLogger = require('memdb-logger');
var logger = memdbLogger.getLogger('memdb', __filename);

var startServer = function(opts){
    logger = memdbLogger.getLogger('memdb', __filename, 'shard:' + opts.shardId);

    var Database = require('./database');
    var server = require('socket.io')();

    var db = new Database(opts);
    db.start().then(function(){
        logger.warn('server started');
    }, function(err){
        logger.error(err.stack);
        process.exit(1);
    });

    server.on('connection', function(socket){
        var connId = db.connect();
        var remoteAddr = socket.conn.remoteAddress;

        socket.on('req', function(msg){
            logger.info('[conn:%s] %s => %j', connId, remoteAddr, msg);
            var resp = {seq : msg.seq};

            P.try(function(){
                if(msg.method === 'info'){
                    return {
                        connId : connId
                    };
                }
                else{
                    return db.execute(connId, msg.method, msg.args);
                }
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
                var level = resp.err ? 'error' : 'info';
                logger[level]('[conn:%s] %s <= %j', connId, remoteAddr, resp);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        socket.on('disconnect', function(){
            P.try(function(){
                return db.disconnect(connId);
            })
            .then(function(){
                logger.info('[conn:%s] %s disconnected', connId, remoteAddr);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        logger.info('[conn:%s] %s connected', connId, remoteAddr);
    });

    server.listen(opts.port);

    var _isShutingDown = false;
    var shutdown = function(){
        logger.warn('receive shutdown signal');

        if(_isShutingDown){
            return;
        }
        _isShutingDown = true;

        return P.try(function(){
            server.close();

            return db.stop();
        })
        .catch(function(e){
            logger.error(e.stack);
        })
        .finally(function(){
            logger.warn('server closed');
            setTimeout(function(){
                process.exit(0);
            }, 200);
        });
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
};

var showUsage = function(){
    var content = 'MemDB - Distributed transactional in memory database\n\n' +
                'Options:\n' +
                '  -c, --conf path      Specify config file path (must with .json extension)\n' +
                '  -s, --shard shardId  Start specific shard\n' +
                '  -d, --daemon         Start as daemon\n' +
                '  -h, --help           Display this help';
    console.log(content);
};

if (require.main === module) {
    process.on('uncaughtException', function(err) {
        logger.error('Uncaught exception: %s', err.stack);
    });

    var argv = minimist(process.argv.slice(2));
    if(argv.help || argv.h){
        showUsage();
        process.exit(0);
    }

    var searchPaths = [];
    var confPath = argv.conf || argv.c || null;
    if(confPath){
        searchPaths.push(confPath);
    }
    searchPaths = searchPaths.concat(['./memdb.json', '~/.memdb.json', '/etc/memdb.json']);

    var conf = null;
    for(var i=0; i<searchPaths.length; i++){
        if(fs.existsSync(searchPaths[i])){
            conf = require(path.resolve(searchPaths[i]));
            break;
        }
    }
    if(!conf){
        console.error('Error: config file not found! %j', searchPaths);
        process.exit(1);
    }

    var shardId = argv.shard || argv.s || null;
    if(!shardId){
        console.error('Please specify shardId with --shard');
        process.exit(1);
    }

    // Start specific shard
    var shardConfig = conf.shards && conf.shards[shardId];
    if(!shardConfig){
        console.error('Shard %s not exist in config', shardId);
        process.exit(1);
    }

    // Override shard specific config
    for(var key in shardConfig){
        conf[key] = shardConfig[key];
    }
    delete conf.shards;

    if(conf.promise && conf.promise.longStackTraces){
        P.longStackTraces();
    }

    // Configure log
    var logConf = conf.log || {};

    var logPath = logConf.path || '/tmp';
    console.log('all output going to: %s/memdb*.log', logPath);

    memdbLogger.configure(path.join(__dirname, 'log4js.json'), {shardId : shardId, base : logPath});

    var level = logConf.level || 'INFO';
    memdbLogger.setGlobalLogLevel(memdbLogger.levels[level]);

    if(argv.d || argv.daemon){
        //Become daemon
        require('daemon')();
    }

    conf.shardId = shardId;
    startServer(conf);
}
