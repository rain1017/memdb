'use strict';

module.exports = {
    backend : {
        engine : 'mongodb',
        url : 'mongodb://localhost/memdb-test',
    },

    locking : {
        host : '127.0.0.1',
        port : 6379,
        db : 1,
    },

    slave : {
        host : '127.0.0.1',
        port : 6379,
        db : 1,
    },

    log : {
        level : 'WARN',
    },

    promise : {
        longStackTraces : false,
    },

    collections : {
        player : {
            indexes : [
                {
                    keys : ['areaId'],
                    valueIgnore : {
                        areaId : ['', -1],
                    },
                },
                {
                    keys : ['deviceType', 'deviceId'],
                    unique : true,
                },
            ]
        }
    },

    shards : {
        s1 : {
            host : '127.0.0.1',
            port : 31017,
        },
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
    },
};
