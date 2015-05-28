'use strict';

var P = require('bluebird');
var should = require('should');
var logger = require('memdb-logger').getLogger('test', __filename);
var BackendLocker = require('../../app/backendlocker');
var env = require('../env');

describe('backendlocker test', function(){

    it('locker / heartbeat', function(cb){
        var docKey = 'player$p1', shardId = 's1';

        var locker = new BackendLocker({
                            host : env.config.shards.s1.locking.host,
                            port : env.config.shards.s1.locking.port,
                            db : env.config.shards.s1.locking.db,
                            shardId : shardId,
                            heartbeatTimeout : 2000,
                            heartbeatInterval : 1000,
                            });

        return P.try(function(){
            return locker.start();
        })
        .then(function(){
            return locker.tryLock(docKey);
        })
        .then(function(){
            return locker.getHolderId(docKey)
            .then(function(ret){
                ret.should.eql(shardId);
            });
        })
        .then(function(ret){
            return locker.isHeld(docKey)
            .then(function(ret){
                ret.should.be.true; //jshint ignore:line
            });
        })
        .then(function(){
            // Can't lock again
            return locker.tryLock(docKey)
            .then(function(ret){
                ret.should.be.false; //jshint ignore:line
            });
        })
        .then(function(){
            return locker.unlock(docKey);
        })
        .finally(function(){
            return locker.stop();
        })
        .nodeify(cb);
    });
});
