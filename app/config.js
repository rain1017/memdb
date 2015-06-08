'use strict';

var P = require('bluebird');
var path = require('path');
var fs = require('fs');
var memdbLogger = require('memdb-logger');
var logger = memdbLogger.getLogger('memdb', __filename);
var utils = require('./utils');

var _config = null;

exports.init = function(confPath, shardId){
    var searchPaths = [];
    if(confPath){
        searchPaths.push(confPath);
    }

    var homePath = process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE;

    searchPaths = searchPaths.concat(['./memdb.json', path.join(homePath, '.memdb.json'), '/etc/memdb.json']);

    var conf = null;
    for(var i=0; i<searchPaths.length; i++){
        if(fs.existsSync(searchPaths[i])){
            conf = require(path.resolve(searchPaths[i]));
            break;
        }
    }
    if(!conf){
        throw new Error('config file not found - ' + searchPaths);
    }

    // Configure promise
    if(conf.promise && conf.promise.longStackTraces){
        P.longStackTraces();
    }

    // Configure log
    var logConf = conf.log || {};

    var logPath = logConf.path || '/tmp';

    console.log('all output going to: %s/memdb-%s.log', logPath, shardId || '$');
    memdbLogger.configure(path.join(__dirname, 'log4js.json'), {shardId : shardId || '$', base : logPath});

    var level = logConf.level || 'INFO';
    memdbLogger.setGlobalLogLevel(memdbLogger.levels[level]);

    _config = conf;
};

exports.getShardIds = function(){
    if(!_config){
        throw new Error('please config.init first');
    }
    return Object.keys(_config.shards);
};

exports.shardConfig = function(shardId){
    if(!_config){
        throw new Error('please config.init first');
    }

    var conf = utils.clone(_config);

    var shardConf = conf.shards && conf.shards[shardId];
    if(!shardConf){
        throw new Error('shard ' + shardId + ' not configured');
    }
    // Override shard specific config
    for(var key in shardConf){
        conf[key] = shardConf[key];
    }
    delete conf.shards;

    conf.shardId = shardId;
    return conf;
};
