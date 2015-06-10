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
        url : 'mongodb://localhost/memdb-test', // mongodb connect string
        options : {}, // mongodb connect options
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

    // Delay for flush changes to backend storage
    // set it to large value to improve performance if the data delay in backend storage is not an issue.
    persistentDelay : 300000, // number in ms, default 300,000

    // Idle time before document is removed from memory.
    // Larger value can improve performance but use more memory.
    // Set it to large value if the documents accessed via this hard is limited.
    // Do not access too many different documents in a short time, which may exhault memory and trigger heavy GC operation.
    idleTimeout : 600000, // number in ms, default 600,000

    // GC will be triggered when memory usage reach this limit
    // GC can be very heavy, please adjust idleTimeout to avoid GC.
    memoryLimit : 1024, // number in MB, default 1024

    // Disable redis replica, DO NOT turn on this in production.
    disableSlave : false, // default false



    // Collection settings, modify it on your need
    collections : {
        // Collection name
        player : {
            // Index setting, modify it on your need
            indexes : [
                {
                    // Index keys
                    keys : ['areaId'],
                    // Value exclude from index. Values like '', -1 occurs too often, which can make the index too large.
                    // 'null' or 'undefined' is ignored by default.
                    valueIgnore : {
                        areaId : ['', -1],
                    },
                },
                {
                    // Index keys (compound index)
                    keys : ['deviceType', 'deviceId'],
                    // Unique constraint for the index
                    unique : true,
                },
            ]
        }
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
    }
};
