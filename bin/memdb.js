#!/usr/bin/env node
'use strict';

var P = require('bluebird');
var minimist = require('minimist');
var memdbLogger = require('memdb-logger');
var util = require('util');
var repl = require('repl');
var memdb = require('../lib');

// turn off logger
memdbLogger.setGlobalLogLevel(memdbLogger.levels.OFF);

var helpContent = '\
Usage: memdb [options]\n\
Options:\n\
  -h, --host ip     memdb shard ip\n\
  -p, --port port   memdb shard port\n\
  --help            display help\n\n\
Example: \n\
memdb -h127.0.0.1 -p31017\n\
';


var startRepl = function(conn){

    var deferred = P.defer();

    var server = repl.start({
        prompt: 'memdb> ',
        terminal: true,
        input: process.stdin,
        output: process.stdout,
        useGlobal : false,
        ignoreUndefined : false,
        writer : JSON.stringify,
    });

    var originEval = server.eval;

    server.eval = function(cmd, context, filename, cb){
        originEval.call(server, cmd, context, filename, function(err, ret){
            if(P.is(ret)){
                //console.log(cmd);
                ret.nodeify(function(err, ret){
                    if(err){
                        err = err.split('\n')[0];
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
        return conn.close()
        .then(function(){
            deferred.resolve();
        }, function(e){
            deferred.reject(e);
        });
    });

    server.context.db = conn;

    return deferred.promise;
};

if (require.main === module) {
    console.log('MemDB shell\n');

    var argv = minimist(process.argv.slice(2));
    if(argv.help){
        console.log(helpContent);
        process.exit(0);
    }

    var host = argv.host || argv.h || '127.0.0.1';
    var port = argv.port || argv.p || 31017;

    var conn = null;

    P.try(function(){
        return memdb.connect({host : host, port : port, idleTimeout : 0});
    })
    .then(function(ret){
        console.log('connected to %s:%s', host, port);
        return startRepl(ret);

    }, function(e){
        console.error('failed to connect to %s:%s', host, port);
        process.exit(1);
    })
    .then(function(){
        console.log('Bye');
        process.exit(0);
    })
    .catch(function(e){
        console.error(e.stack);
        process.exit(1);
    });
}


