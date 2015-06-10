'use strict';
/*
 * MemDB config template
 *
 * Please modify it on your needs
 * copy this file to /etc/memdb.js or ~/.memdb.js
 *
 * This is plain javascript, you can add any js code here, just export the config
 */

module.exports = {
    // *** global settings for all shards ***

    // Global backend storage, all shards must connect to the same mongodb (cluster)
    backend : {
        engine : 'mongodb', // should be 'mongodb'
        url : 'mongodb://localhost/memdb-test',
        options : {},
    },

    // Global locking redis, all shards must connect to the same redis (cluster)
    locking : {
        host : '127.0.0.1',
        port : 6379,
        db : 0,
    },

    // Global event redis, all shards must connect to the same redis
    event : {
        host : '127.0.0.1',
        port : 6379,
        db : 0,
    },

    // Data replication redis, one redis instance for each shard
    // You can override this in shard settings to choice different slave for each shard
    slave : {
        host : '127.0.0.1',
        port : 6379,
        db : 1,
    },


    // Collection settings, modify it on your need
    collections : {
        // Collection name
        player : {
            // Index setting, modify it on your need
            indexes : [
                {
                    // Index keys
                    keys : ['areaId'],
                    // Value excluded from index
                    valueIgnore : {
                        areaId : ['', -1],
                    },
                },
                {
                    // Index keys
                    keys : ['deviceType', 'deviceId'],
                    // Is unique
                    unique : true,
                },
            ]
        }
    },

    // Log settings
    log : {
        // Log file path
        path : '/tmp',
        // Log Level (one of 'ALL', 'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR')
        // Please set to WARN on production
        level : 'WARN',
    },

    // Promise settings
    promise : {
        // Enable long stack trace, disable it on production
        longStackTraces : false,
    },


    // *** Shard specific settings ***

    // This will override global settings on specifid shard
    shards : {
        // shardId
        s1 : {
            // server IP
            host : '127.0.0.1',
            // listen port
            port : 31017,
            // bind Ip
            bindIp : '0.0.0.0',

            // Add any shard specific settings here
            // slave : {
            //     host : '127.0.0.1',
            //     port : 6379,
            //     db : 1,
            // },
        },

        // Add more shards
        s2 : {
            host : '127.0.0.1',
            port : 31018,
        },
        s3 : {
            host : '127.0.0.1',
            port : 31019,
        },
        s4 : {
            host : '127.0.0.1',
            port : 31020,
        },
    }
};
