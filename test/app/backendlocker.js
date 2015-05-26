'use strict';

var P = require('bluebird');
var should = require('should');
var logger = require('memdb-logger').getLogger('test', __filename);
var BackendLocker = require('../../app/backendlocker');
var env = require('../env');

describe('backendlocker test', function(){

    it('lock/unlock', function(cb){
        var docId = 'doc1', shardId = 's1';

        var locker = new BackendLocker({
                            host : env.config.shards.s1.locking.host,
                            port : env.config.shards.s1.locking.port,
                            db : env.config.shards.s1.locking.db,
                            shardId : shardId,
                            });

        return P.try(function(){
            return locker.unlockAll();
        })
        .then(function(){
            return locker.lock(docId);
        })
        .then(function(){
            return locker.getHolderId(docId)
            .then(function(ret){
                ret.should.eql(shardId);
            });
        })
        .then(function(){
            return locker.getHolderIdMulti([docId, 'nonExistDoc'])
            .then(function(ret){
                ret.should.eql([shardId, null]);
            });
        })
        .then(function(ret){
            return locker.isHeld(docId)
            .then(function(ret){
                ret.should.be.true; //jshint ignore:line
            });
        })
        .then(function(){
            return locker.ensureHeld(docId);
        })
        .then(function(){
            // Can't lock again
            return locker.tryLock(docId)
            .then(function(ret){
                ret.should.be.false; //jshint ignore:line
            });
        })
        .then(function(){
            return locker.unlock(docId);
        })
        .then(function(){
            //Should throw error
            return locker.ensureHeld(docId)
            .then(function(){
                throw new Error('Should throw error');
            }, function(e){
                //expected
            });
        })
        .finally(function(){
            return P.try(function(){
                return locker.unlockAll();
            }).then(function(){
                locker.close();
            });
        })
        .nodeify(cb);
    });
});
