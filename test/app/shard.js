'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var Shard = require('../../app/shard');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('shard test', function(){
    beforeEach(env.flushdb);
    after(env.flushdb);

    it('load/unload/find/update/insert/remove/commit/rollback', function(cb){
        var shard = new Shard(env.dbConfig('s1'));
        var connId = 'c1', key = 'user$1', doc = {_id : '1', name : 'rain', age : 30};

        return P.try(function(){
            return shard.start();
        })
        .then(function(){
            // pre create collection for performance
            return shard.backend.conn.createCollectionAsync('user');
        })
        .then(function(){
            // should auto load
            return shard.lock(connId, key);
        })
        .then(function(){
            shard._isLoaded(key).should.eql(true);

            // insert doc
            return shard.insert(connId, key, doc);
        })
        .then(function(){
            // request to unload doc
            shard.globalEvent.emit('shard$s1', 'unload', key);
        })
        .delay(20)
        .then(function(){
            // should still loaded, waiting for commit
            shard._isLoaded(key).should.eql(true);

            return shard.commit(connId, key);
        })
        .delay(100)
        .then(function(){
            // should unloaded now and saved to backend
            shard._isLoaded(key).should.eql(false);

            // load again
            return shard.lock(connId, key);
        })
        .then(function(){
            // Already loaded, should return immediately
            shard.find(connId, key).should.eql(doc);

            // load again for write
            return shard.lock(connId, key);
        })
        .then(function(){
            shard.update(connId, key, {age : 31});
            shard.find(connId, key, 'age').age.should.eql(31);

            // rollback to original value
            shard.rollback(connId, key);
            shard.find(connId, key, 'age').age.should.eql(30);

            return shard.lock(connId, key);
        })
        .then(function(){
            shard.remove(connId, key);
            (shard.find(connId, key) === null).should.be.true; // jshint ignore:line
            return shard.commit(connId, key);
        })
        .then(function(){
            // request to unload
            shard.globalEvent.emit('shard$s1', 'unload', key);
        })
        .delay(100)
        .then(function(){
            // load again
            return P.try(function(){
                return shard.lock(connId, key);
            })
            .then(function(){
                var doc = shard.find(connId, key);
                (doc === null).should.be.true; // jshint ignore:line
            });
        })
        .then(function(){
            return shard.stop();
        })
        .nodeify(cb);
    });

    it('backendLock between multiple shards', function(cb){
        var config = env.dbConfig('s1');
        config.backendLockRetryInterval = 500; // This is required for this test
        var shard1 = new Shard(config);
        var shard2 = new Shard(env.dbConfig('s2'));

        var key = 'user$1', doc = {_id : '1', name : 'rain', age : 30};
        return P.try(function(){
            return P.all([shard1.start(), shard2.start()]);
        })
        .then(function(){
            // pre create collection for performance
            return shard1.backend.conn.createCollectionAsync('user');
        })
        .then(function(){
            return P.all([
                // shard1:c1
                P.try(function(){
                    // load by shard1
                    return shard1.lock('c1', key);
                })
                .delay(100) // wait for shard2 request
                .then(function(){
                    // should unloading now (wait for unlock)
                    // already locked, so will not block
                    return shard1.lock('c1', key);
                })
                .then(function(){
                    shard1.insert('c1', key, doc);
                    return shard1.commit('c1', key);
                    // unlocked, unloading should continue
                }),

                // shard2:c1
                P.delay(20) // shard1 should load first
                .then(function(){
                    // This will block until shard1 unload the key
                    return shard2.lock('c1', key);
                })
                .then(function(){
                    var doc = shard2.find('c1', key);
                    doc.should.eql(doc);

                    shard2.remove('c1', key);
                    return shard2.commit('c1', key);
                }),

                // shard1:c2
                P.delay(50)
                .then(function(){
                    // since shard1 is unloading, lock will block until unloaded
                    // and then will load again (after shard2 unload)
                    return shard1.lock('c2', key);
                })
                .then(function(){
                    // should read shard2 saved value
                    (shard1.find('c2', key) === null).should.be.true; // jshint ignore:line
                })
                .then(function(){
                    return shard1.commit('c2', key);
                })
            ]);
        })
        .then(function(){
            return P.all([shard1.stop(), shard2.stop()]);
        })
        .nodeify(cb);
    });

    it('backendLock consistency fix', function(cb){
        var shard1 = new Shard(env.dbConfig('s1'));
        var config = env.dbConfig('s2');
        config.backendLockTimeout = 500;
        var shard2 = new Shard(config);

        var key = 'user$1', doc = {_id : '1', name : 'rain', age : 30};
        var errCount = 0;

        return P.try(function(){
            return P.all([shard1.start(), shard2.start()]);
        })
        .then(function(){
            return P.try(function(){
                // shard1 get a redundant lock
                return shard1.backendLocker.tryLock(key);
            })
            .then(function(){
                // should also access
                return shard2.lock('c1', key);
            });
        })
        .then(function(){
            return P.all([shard1.stop(), shard2.stop()]);
        })
        .nodeify(cb);
    });
});
