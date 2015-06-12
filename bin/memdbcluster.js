#!/usr/bin/env node
'use strict';

var minimist = require('minimist');
var P = require('bluebird');
var path = require('path');
var config = require('../app/config');
var util = require('util');
var utils = require('../app/utils');

var helpContent = '\
MemDB - Distributed transactional in memory database\n\n\
Usage: memdbcluster [start | stop | status] [options]\n\
Options:\n\
  -c, --conf path      Config file path\n\
  -s, --shard shardId  Operate on specific shard only\n\
  -h, --help           Display this help\n\n\
WARN: Make sure memdb is installed using the same folder and same config on all servers\
';

var start = function(confPath, shardConfig){
    var memdbd = path.join(__dirname, 'memdbd.js');
    var nodePath = process.execPath;
    var cmd = util.format('%s %s --conf=%s --shard=%s --daemon', nodePath, memdbd, confPath || '', shardConfig.shardId);
    return utils.remoteExec(shardConfig.host, cmd, {user : shardConfig.user});
};

var stop = function(shardConfig){
    var cmd = util.format('kill `lsof -i:%s -t`', shardConfig.port);
    return utils.remoteExec(shardConfig.host, cmd, {successCodes : [0, 1], user : shardConfig.user});
};

var status = function(shardConfig){
    var cmd = util.format('lsof -i:%s -t', shardConfig.port);
    return utils.remoteExec(shardConfig.host, cmd, {successCodes : [0, 1], user : shardConfig.user})
    .then(function(output){
        output = output.trim();
        return !!output;
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
    var shardId = argv.shard || argv.s;
    config.init(confPath);
    var shardIds = shardId ? [shardId] : config.getShardIds();

    P.try(function(){
        if(cmd === 'start'){
            return P.each(shardIds, function(shardId){
                var shardConfig = config.shardConfig(shardId);
                console.log('starting %s on %s:%s', shardConfig.shardId, shardConfig.host, shardConfig.port);
                return start(confPath, shardConfig)
                .catch(function(e){
                    console.error(e.stack);
                });
            });
        }
        else if(cmd === 'stop'){
            return P.each(shardIds, function(shardId){
                var shardConfig = config.shardConfig(shardId);
                console.log('stopping %s on %s:%s', shardConfig.shardId, shardConfig.host, shardConfig.port);
                return stop(shardConfig)
                .catch(function(e){
                    console.error(e.stack);
                });
            });
        }
        else if(cmd === 'status'){
            return P.each(shardIds, function(shardId){
                var shardConfig = config.shardConfig(shardId);
                return status(shardConfig)
                .then(function(ret){
                    var status = ret ? 'running' : 'down';
                    console.log('%s (%s:%s)\t%s', shardConfig.shardId, shardConfig.host, shardConfig.port, status);
                });
            });
        }
        else{
            throw new Error('invalid command - %s', cmd);
        }
    })
    .catch(function(e){
        console.error(e.stack);
    })
    .finally(process.exit);
}
