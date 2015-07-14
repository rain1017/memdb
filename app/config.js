'use strict';

var P = require('bluebird');
var path = require('path');
var fs = require('fs');
var mkdirp = require('mkdirp');
var memdbLogger = require('memdb-logger');
var logger = memdbLogger.getLogger('memdb', __filename);
var utils = require('./utils');

var _config = null;

exports.init = function(confPath, shardId){
    var searchPaths = [];
    var homePath = process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE;

    searchPaths = confPath ? [confPath] : [path.join(homePath, '.memdb/memdb.conf.js'), '/etc/memdb.conf.js'];

    var conf = null;
    for(var i=0; i<searchPaths.length; i++){
        if(fs.existsSync(searchPaths[i])){
            confPath = path.resolve(searchPaths[i]);
            conf = require(confPath);
            exports.path = confPath;
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

    var logPath = logConf.path || path.join(homePath, '.memdb/log');
    mkdirp(logPath);

    console.log('log path: %s', logPath);
    memdbLogger.configure(path.join(__dirname, 'log4js.json'), {shardId : shardId || '$', base : logPath});

    var level = logConf.level || 'INFO';
    memdbLogger.setGlobalLogLevel(memdbLogger.levels[level]);

    // heapdump
    if(conf.heapdump){
        require('heapdump');
    }

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

    conf.shardId = shardId;
    return conf;
};

exports.clusterConfig = function(){
    return _config;
};
