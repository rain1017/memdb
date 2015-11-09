// Copyright 2015 rain1017.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

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
                            host : env.config.locking.host,
                            port : env.config.locking.port,
                            db : env.config.locking.db,
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
