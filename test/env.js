// Copyright 2015 The MemDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

'use strict';

var P = require('bluebird');
var path = require('path');
var os = require('os');
var fs = require('fs');
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
    if(typeof(cb) === 'function'){
        cb();
    }
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
