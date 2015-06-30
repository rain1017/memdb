'use strict';

var P = require('bluebird');
var mongodb = P.promisifyAll(require('mongodb'));
var child_process = require('child_process');
var path = require('path');
var fs = require('fs');
var logger = require('memdb-logger').getLogger('test', __filename);

// Memdb launcher utility for test purpose only

var Launcher = function(opts){
    opts = opts || {};
    this.memdbdPath = opts.memdbd || '/usr/local/bin/memdbd';
    if(!fs.existsSync(this.memdbdPath)){
        throw new Error('memdbd not found');
    }
    this.confPath = opts.conf;
    this.config = require(this.confPath);
    this.servers = {}; // {shardId : server}
};

// start shards in localhost
Launcher.prototype.startCluster = function(shardIds, configOverrideFunc){
    if(!shardIds){
        shardIds = Object.keys(this.config.shards);
    }
    if(!Array.isArray(shardIds)){
        shardIds = [shardIds];
    }

    var newConfigPath = this.confPath;
    if(typeof(configOverrideFunc) === 'function'){
        var newConfig = JSON.parse(JSON.stringify(this.config)); //deep copy
        configOverrideFunc(newConfig);
        newConfigPath = '/tmp/.memdb-test.json';
        fs.writeFileSync(newConfigPath, JSON.stringify(newConfig));
    }

    var self = this;

    return P.map(shardIds, function(shardId){
        var shardConfig = self.config.shards[shardId];
        if(!shardConfig){
            throw new Error('shard ' + shardId + ' not configured');
        }
        if(shardConfig.host !== '127.0.0.1' && shardConfig.host !== 'localhost'){
            throw new Error('shard host be localhost');
        }
        if(!!self.servers[shardId]){
            throw new Error('shard ' + shardId + ' already started');
        }

        var args = ['--conf=' + newConfigPath, '--shard=' + shardId];
        var server = child_process.fork(self.memdbdPath, args);

        var deferred = P.defer();
        server.on('message', function(msg){
            if(msg === 'start'){
                deferred.resolve();
                logger.warn('shard %s started', shardId);
            }
        });

        self.servers[shardId] = server;
        return deferred.promise;
    });
};

Launcher.prototype.stopCluster = function(){
    var self = this;

    return P.map(Object.keys(self.servers), function(shardId){
        var server = self.servers[shardId];

        var deferred = P.defer();
        server.on('exit', function(code, signal){
            if(code === 0){
                deferred.resolve();
            }
            else{
                deferred.reject('shard ' + shardId + ' returned non-zero code');
            }
            delete self.servers[shardId];
            logger.warn('shard %s stoped', shardId);
        });
        server.kill();
        return deferred.promise;
    });
};

Launcher.prototype.flushdb = function(){
    if(Object.keys(this.servers).length > 0){
        throw new Error('can not flushdb when server is running');
    }

    var self = this;
    return P.try(function(){
        return P.promisify(mongodb.MongoClient.connect)(self.config.backend.url)
        .then(function(db){
            return db.dropDatabaseAsync()
            .then(function(){
                return db.closeAsync();
            });
        });
    })
    .then(function(){
        logger.info('flushed db');
    });
};

module.exports = {
    Launcher : Launcher,
};
