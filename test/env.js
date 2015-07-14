'use strict';

var P = require('bluebird');
var path = require('path');
var os = require('os');
var child_process = require('child_process');
var memdbConfig = require('../app/config');
var utils = require('../app/utils');
var memdbLogger = require('memdb-logger');
var logger = memdbLogger.getLogger('test', __filename);

var configPath = path.join(__dirname, './memdb.conf.js');
memdbConfig.init(configPath);
var config = memdbConfig.clusterConfig();

var memdbClusterPath = path.join(__dirname, '../bin/memdbcluster');

exports.startCluster = function(shardIds, configOverrideFunc){
    var newConfigPath = configPath;
    if(typeof(configOverrideFunc) === 'function'){
        var newConfig = utils.clone(config);
        configOverrideFunc(newConfig);
        newConfigPath = path.join(os.tmpDir(), 'memdb-test.conf.json');
        fs.writeFileSync(newConfigPath, JSON.stringify(newConfig));
    }

    var args = [memdbClusterPath, 'start', '--conf=' + newConfigPath];
    if(shardIds){
        if(!Array.isArray(shardIds)){
            shardIds = [shardIds];
        }
        shardIds.forEach(function(shardId){
            args.push('--shard=' + shardId);
        });
    }

    var output = child_process.execFileSync(process.execPath, args).toString();
    logger.info(output.toString());
};

exports.stopCluster = function(){
    var output = child_process.execFileSync(process.execPath, [memdbClusterPath, 'stop', '--conf=' + configPath]);
    logger.info(output.toString());
};

exports.flushdb = function(cb){
    var output = child_process.execFileSync(process.execPath, [memdbClusterPath, 'drop', '--conf=' + configPath]);
    logger.info(output.toString());
    cb();
};

exports.shardConfig = function(shardId){
    return memdbConfig.shardConfig(shardId);
};

exports.runScript = function(script, args){
    var proc = child_process.fork(script, args);
    var deferred = P.defer();

    proc.on('exit', function(code){
        if(code === 0){
            deferred.resolve();
        }
        else{
            deferred.reject('script ' + script + ' returned non-zero code');
        }
    });
    return deferred.promise;
};

exports.config = config;
exports.configPath = configPath;
