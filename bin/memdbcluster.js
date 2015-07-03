#!/usr/bin/env node
'use strict';

var minimist = require('minimist');
var P = require('bluebird');
var path = require('path');
var util = require('util');
var utils = require('../app/utils');

var helpContent = '\
MemDB - Distributed transactional in memory database\n\n\
Usage: memdbcluster [command] [options]\n\n\
Commands:\n\
  start     Start cluster\n\
  stop      Stop cluster\n\
  status    Show cluster status\n\
  drop      Drop all data in cluster\n\n\
Options:\n\
  -c, --conf path      Config file path\n\
  -s, --shard shardId  Operate on specific shard only\n\
  -h, --help           Display this help\n\n\
WARN: Make sure memdb is installed using the same folder and same config on all servers\n';

var start = function(confPath, shardConfig){
    var memdbd = path.join(__dirname, 'memdbd.js');
    var nodePath = process.execPath;
    var cmd = util.format('%s %s --conf=%s --shard=%s --daemon', nodePath, memdbd, confPath || '', shardConfig.shardId);
    return utils.remoteExec(shardConfig.host, cmd, {user : shardConfig.user});
};

var stop = function(shardConfig){
    var cmd = 'PID=`lsof -i:%s -sTCP:LISTEN -t`;\
               if [ $PID ]; then\
                  kill $PID;\
                  while ps -p $PID > /dev/null; do\
                    sleep 0.2;\
                  done;\
               fi';

    cmd = util.format(cmd, shardConfig.port);
    return utils.remoteExec(shardConfig.host, cmd, {user : shardConfig.user});
};

var mongodb = null;
var drop = function(backendConf){
    if(!backendConf){
        throw new Error('global backend config not found');
    }
    if(!mongodb){
        mongodb = P.promisifyAll(require('mongodb'));
    }

    return P.promisify(mongodb.MongoClient.connect)(backendConf.url)
    .then(function(db){
        return db.dropDatabaseAsync()
        .then(function(){
            return db.closeAsync();
        });
    });
};

var backendLocker = null;
var getActiveShards = function(lockingConf){
    if(!lockingConf){
        throw new Error('global locking config not found');
    }
    if(!backendLocker){
        backendLocker = require('../app/backendlocker');
    }

    var bl = new backendLocker(lockingConf);
    return P.try(function(){
        return bl.start();
    })
    .then(function(){
        return bl.getActiveShards();
    })
    .then(function(ret){
        bl.stop();
        return ret;
    });
};

if (require.main === module) {
    var argv = minimist(process.argv.slice(2));

    var cmd = process.argv[2];

    if(process.argv.length <= 2 || argv.help || argv.h){
        console.log(helpContent);
        process.exit(0);
    }

    var confPath = argv.conf || argv.c;
    if(confPath){
        confPath = path.resolve(confPath);
    }

    var config = require('../app/config');
    config.init(confPath);

    require('memdb-logger').setGlobalLogLevel('OFF'); //turn off logger

    var shardIds = argv.shard || argv.s;
    if(!shardIds){
        shardIds = config.getShardIds();
    }
    else if(!Array.isArray(shardIds)){
        shardIds = [shardIds];
    }

    // prevent from exit
    process.on('SIGTERM', function(){});
    process.on('SIGINT', function(){});

    P.try(function(){
        if(cmd === 'start'){
            return P.map(shardIds, function(shardId){
                var shardConfig = config.shardConfig(shardId);
                console.log('starting %s on %s:%s...', shardId, shardConfig.host, shardConfig.port);
                return start(confPath, shardConfig)
                .then(function(){
                    console.log('%s started', shardId);
                }, function(e){
                    console.error(e.stack);
                });
            });
        }
        else if(cmd === 'stop'){
            return P.map(shardIds, function(shardId){
                var shardConfig = config.shardConfig(shardId);
                console.log('stopping %s on %s:%s...', shardConfig.shardId, shardConfig.host, shardConfig.port);
                return stop(shardConfig)
                .then(function(){
                    console.log('%s stoped', shardId);
                }, function(e){
                    console.error(e.stack);
                });
            });
        }
        else if(cmd === 'status'){
            return getActiveShards(config.clusterConfig().locking)
            .then(function(activeShardIds){
                shardIds.forEach(function(shardId){
                    var shardConfig = config.shardConfig(shardId);
                    var display = activeShardIds.indexOf(shardId) !== -1 ? 'running' : 'down';
                    console.log('%s (%s:%s)\t%s', shardId, shardConfig.host, shardConfig.port, display);
                });
            });
        }
        else if(cmd === 'drop'){
            return getActiveShards(config.clusterConfig().locking)
            .then(function(activeShardIds){
                if(activeShardIds.length > 0){
                    throw new Error('please stop all shards first');
                }
                return drop(config.clusterConfig().backend)
                .then(function(){
                    console.log('all data has been droped');
                });
            });
        }
        else{
            throw new Error('invalid command - %s', cmd);
        }
    })
    .catch(function(e){
        console.error(e.stack);
        process.exit(1);
    })
    .finally(process.exit);
}
