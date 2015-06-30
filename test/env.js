'use strict';

var P = require('bluebird');
var path = require('path');
var child_process = require('child_process');
var Launcher = require('../lib').test.Launcher;
var memdbConfig = require('../app/config');
var memdbLogger = require('memdb-logger');
var logger = memdbLogger.getLogger('test', __filename);

var configPath = path.join(__dirname, '../.memdb.js');
memdbConfig.init(configPath);
var config = memdbConfig.clusterConfig();

var launcher = new Launcher({
                        memdbd : path.join(__dirname, '../bin/memdbd.js'),
                        conf : configPath,
                    });

exports.startCluster = function(shardIds, configOverrideFunc){
    return launcher.startCluster(shardIds, configOverrideFunc);
};

exports.stopCluster = function(){
    return launcher.stopCluster();
};

exports.flushdb = function(cb){
    var promise = launcher.flushdb();
    return (typeof(cb) === 'function') ? promise.nodeify(cb) : promise;
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
