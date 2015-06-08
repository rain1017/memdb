#!/usr/bin/env node
'use strict';

var P = require('bluebird');
var minimist = require('minimist');
var logger = require('memdb-logger').getLogger('memdb', __filename);
var config = require('../app/config');
var server = require('../app/server');

var helpContent = '\
MemDB - Distributed transactional in memory database\n\n\
Usage: memdbd [options]\n\
Options:\n\
  -c, --conf path      Config file path (must with .json extension)\n\
  -s, --shard shardId  Start specific shard\n\
  -d, --daemon         Start as daemon\n\
  -h, --help           Display this help\n\
';


if (require.main === module) {
    var argv = minimist(process.argv.slice(2));
    if(process.argv.length <= 2 || argv.help || argv.h){
        console.log(helpContent);
        process.exit(0);
    }

    var confPath = argv.conf || argv.c;
    var shardId = argv.shard || argv.s;

    var conf = config.init(confPath, shardId);

    if(!shardId){
        throw new Error('Please specify shardId with --shard');
    }
    var shardConfig = config.shardConfig(shardId);

    if(argv.d || argv.daemon){
        //Become daemon
        require('daemon')();
    }

    server.start(shardConfig);
}
