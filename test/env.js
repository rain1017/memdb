'use strict';

var memdb = require('../lib');
var P = require('bluebird');
var child_process = require('child_process');
var path = require('path');
var fs = require('fs');
var redis = P.promisifyAll(require('redis'));
var mongodb = P.promisifyAll(require('mongodb'));
var memdbConfig = require('../app/config');
var memdbLogger = require('memdb-logger');
var logger = memdbLogger.getLogger('test', __filename);

var configPath = path.join(__dirname, '../.memdb.js');
memdbConfig.init(configPath);
var config = memdbConfig.clusterConfig();

var serverScript = path.join(__dirname, '../bin/memdbd.js');
var _servers = {}; // {shardId : server}

exports.startCluster = function(shardIds, configOverrideFunc){
    if(!shardIds){
        throw new Error('shardIds is missing');
    }
    if(!Array.isArray(shardIds)){
        shardIds = [shardIds];
    }

    var newConfig = JSON.parse(JSON.stringify(config)); //deep copy
    if(typeof(configOverrideFunc) === 'function'){
        configOverrideFunc(newConfig);
    }

    var newConfigPath = '/tmp/.memdb-test.json';
    fs.writeFileSync(newConfigPath, JSON.stringify(newConfig));

    return P.map(shardIds, function(shardId){
        if(!config.shards[shardId]){
            throw new Error('shard ' + shardId + ' not configured');
        }
        if(!!_servers[shardId]){
            throw new Error('shard ' + shardId + ' already started');
        }

        var args = ['--conf=' + newConfigPath, '--shard=' + shardId];
        var server = child_process.fork(serverScript, args);

        var deferred = P.defer();
        server.on('message', function(msg){
            if(msg === 'start'){
                deferred.resolve();
                logger.warn('shard %s started', shardId);
            }
        });

        _servers[shardId] = server;
        return deferred.promise;
    });
};

exports.stopCluster = function(){
    return P.map(Object.keys(_servers), function(shardId){
        var server = _servers[shardId];

        var deferred = P.defer();
        server.on('exit', function(code, signal){
            if(code === 0){
                deferred.resolve();
            }
            else{
                deferred.reject('shard ' + shardId + ' returned non-zero code');
            }
            delete _servers[shardId];
            logger.warn('shard %s stoped', shardId);
        });
        server.kill();
        return deferred.promise;
    });
};

exports.flushdb = function(cb){
    var promise = P.try(function(){
        return P.promisify(mongodb.MongoClient.connect)(config.backend.url, config.backend.options)
        .then(function(db){
            return db.dropDatabaseAsync()
            .then(function(){
                return db.closeAsync();
            });
        });
    })
    .then(function(){
        return P.map([config.locking, config.event, config.slave], function(redisConfig){

            var client = redis.createClient(redisConfig.port, redisConfig.host);
            client.select(redisConfig.db);

            return client.flushdbAsync()
            .then(function(){
                return client.quitAsync();
            });
        });
    })
    .then(function(){
        logger.info('flushed db');
    });

    if(typeof(cb) === 'function'){
        return promise.nodeify(cb);
    }
    else{
        return promise;
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
