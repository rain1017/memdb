#!/usr/bin/env node
'use strict';

var minimist = require('minimist');
var indexbuilder = require('../app/indexbuilder');
var config = require('../app/config');

var helpContent = '\
MemDB Index Builder\n\n\
Usage: memdbindex [rebuild | drop] [options]\n\
Options:\n\
  -c, --conf path       Config file path\n\
  -t, --coll collection Collection name\n\
  -k, --keys key1.key2  Index keys (split with ".")\n';

var getIndexOpts = function(conf, collName, keys){
    var indexes = conf.collections && conf.collections[collName] && conf.collections[collName].indexes;
    if(!indexes){
        return null;
    }
    for(var i in indexes){
        var index = indexes[i];
        if(JSON.stringify(index.keys.sort()) === JSON.stringify(keys.sort())){
            return index;
        }
    }
    return null;
};

if (require.main === module){
    var argv = minimist(process.argv.slice(3));
    var cmd = process.argv[2];
    if(process.argv.length <= 3 || argv.help || argv.h){
        console.log(helpContent);
        process.exit(0);
    }

    config.init(argv.conf || argv.c);
    var conf = config.shardConfig(config.getShardIds()[0]);

    var collName = argv.coll || argv.t;
    if(!collName){
        throw new Error('collection not specified');
    }
    var keys = argv.keys || argv.k;
    if(!keys){
        throw new Error('keys not specified');
    }
    keys = keys.split('.').sort();

    if(cmd === 'rebuild'){
        var opts = getIndexOpts(conf, collName, keys);
        if(!opts){
            throw new Error('specified index is not configured');
        }
        indexbuilder.rebuild(conf, collName, keys, opts);
    }
    else if(cmd === 'drop'){
        indexbuilder.drop(conf, collName, keys);
    }
    else{
        throw new Error('invalid cmd - ' + cmd);
    }
}
