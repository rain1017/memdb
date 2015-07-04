#!/usr/bin/env node
'use strict';

var P = require('bluebird');
var path = require('path');
var minimist = require('minimist');
var memdbLogger = require('memdb-logger');
var util = require('util');
var repl = require('repl');

var helpStart = '\
Usage: memdb [options]\n\
Options:\n\
  -h, --host ip     memdb shard ip\n\
  -p, --port port   memdb shard port\n\
  -s, --shard shardId  shardId\n\
  -c, --conf path      Config file path\n\
  --help            display help\n\n\
Example: \n\
memdb -h127.0.0.1 -p31017\n\
memdb -s s1 -c .memdb.js\n';

var helpCmd = '\n\
Command Reference:\n\n\
    db.find(collection, idOrQuery)\n\
    db.insert(collection, docs)\n\
    db.update(collection, idOrQuery, modifier)\n\
    db.remove(collection, idOrQuery)\n\
    db.commit()\n\
    db.rollback()\n\
    _   last result\n';

var startRepl = function(conn){

    var deferred = P.defer();

    var server = repl.start({
        prompt: 'memdb> ',
        terminal: true,
        input: process.stdin,
        output: process.stdout,
        useGlobal : false,
        ignoreUndefined : true,
    });

    var originEval = server.eval; //jshint ignore:line

    server.eval = function(cmd, context, filename, cb){ //jshint ignore:line
        if(cmd.trim() === 'help'){
            console.log(helpCmd);
            return cb();
        }

        originEval.call(server, cmd, context, filename, function(err, ret){
            if(err){
                console.log(helpCmd);
                return cb(err, ret);
            }

            if(P.is(ret)){
                ret.nodeify(function(err, ret){
                    if(err){
                        err = err.message + '\n (Changes are rolled back)';
                    }
                    cb(err, ret);
                });
            }
            else{
                cb(err, ret);
            }
        });
    };

    server.on('exit', function(){
        conn.removeAllListeners('close');

        return conn.close()
        .then(function(){
            console.log('Bye');
            deferred.resolve();
        }, function(e){
            deferred.reject(e);
        });
    });

    server.context.db = conn;

    conn.on('error', function(e){
        console.error(e.stack);
    });

    conn.on('close', function(){
        console.log('Connection lost');
        deferred.resolve();
    });

    return deferred.promise;
};

if (require.main === module) {
    console.log('MemDB shell');

    var argv = minimist(process.argv.slice(2));
    if(argv.help){
        console.log(helpStart);
        process.exit(0);
    }

    var connectOpts = {idleTimeout : 0};

    var shardId = argv.shard || argv.s;
    var confPath = argv.conf || argv.c;
    if(confPath){
        confPath = path.resolve(confPath);
    }

    if(shardId){
        var config = require('../app/config');
        config.init(confPath);
        var shardConfig = config.shardConfig(shardId);
        connectOpts.host = shardConfig.host;
        connectOpts.port = shardConfig.port;
    }
    else{
        if(confPath){
            throw new Error('shard not specified');
        }
        connectOpts.host = argv.host || argv.h || '127.0.0.1';
        connectOpts.port = argv.port || argv.p || 31017;
    }

    // turn off logger
    memdbLogger.setGlobalLogLevel(memdbLogger.levels.OFF);

    var memdb = require('../lib');
    var conn = null;

    P.try(function(){
        return memdb.connect(connectOpts);
    })
    .then(function(ret){
        console.log('connected to %s:%s', connectOpts.host, connectOpts.port);

        return startRepl(ret);
    }, function(e){
        console.error('failed to connect to %s:%s', connectOpts.host, connectOpts.port);
        process.exit(1);
    })
    .then(function(){
        process.exit(0);
    })
    .catch(function(e){
        console.error(e.stack);
        process.exit(1);
    });
}


