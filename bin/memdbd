#!/usr/bin/env node
'use strict';

var helpContent = '\
MemDB - Distributed transactional in memory database\n\n\
Usage: memdbd [options]\n\
Options:\n\
  -c, --conf path      Config file path\n\
  -s, --shard shardId  Start specific shard\n\
  -d, --daemon         Start as daemon\n\
  -h, --help           Display this help\n';

if (require.main === module) {
    var argv = require('minimist')(process.argv.slice(2));

    if(process.argv.length <= 2 || argv.help || argv.h){
        console.log(helpContent);
        process.exit(0);
    }

    var confPath = argv.conf || argv.c;
    var shardId = argv.shard || argv.s;
    if(!shardId){
        throw new Error('Please specify shardId with --shard');
    }

    //Become daemon
    if((argv.d || argv.daemon) && !process.env.__parentPid){

        process.env.__parentPid = process.pid;

        var opts = {
            stdio : 'ignore',
            env : process.env,
            cwd : process.cwd(),
            detached : true,
        };
        var args = [].concat(process.argv);
        args.shift(); // shift off node

        process.stdout.write('starting ' + shardId + '... ');

        var child = require('child_process').spawn(process.execPath, args, opts);
        child.on('exit', function(code, signal){
            if(code !== 0){
                process.stdout.write('failed\n');
                process.exit(1);
            }
            else{
                process.exit(0);
            }
        });
        child.unref();

        process.on('SIGUSR2', function(){
            process.stdout.write('success\n');
            process.exit(0);
        });

        // prevent from exit
        process.on('SIGTERM', function(){});
        process.on('SIGINT', function(){});
        setTimeout(function(){}, 100000000);

        return;
    }

    var config = require('../app/config');
    var conf = config.init(confPath, shardId);
    var shardConfig = config.shardConfig(shardId);

    require('../app/server').start(shardConfig)
    .then(function(){
        // send signal to parent process
        var parentPid = process.env.__parentPid;
        if(parentPid){
            process.kill(parentPid, 'SIGUSR2');
        }
    }, function(e){
        console.error(e.stack ? e.stack : e);

        require('memdb-logger').shutdown(function(){
            process.exit(1);
        });
    });
}
