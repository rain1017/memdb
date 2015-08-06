// Copyright 2015 The MemDB Authors.
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
var util = require('util');
var path = require('path');
var should = require('should');
var env = require('../env');
var memdb = require('../../lib');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('indexbuilder test', function(){
    beforeEach(env.flushdb);

    it('rebuild', function(cb){
        var shardId = 's1';
        var autoconn = null;

        return P.try(function(){
            return env.startCluster(shardId);
        })
        .then(function(){
            return memdb.autoConnect(env.config)
            .then(function(ret){
                autoconn = ret;
            });
        })
        .then(function(){
            var Player = autoconn.collection('player');
            return autoconn.transaction(function(){
                return P.try(function(){
                    return Player.insert({_id : 'p1', areaId : 'a1'});
                })
                .then(function(){
                    return Player.insert({_id : 'p2', areaId : 'a2'});
                });
            }, shardId);
        })
        .then(function(){
            return autoconn.close();
        })
        .then(function(){
            return env.stopCluster();
        })
        .then(function(){
            var script = path.join(__dirname, '../../bin/memdbindex');
            var args = ['--conf=' + env.configPath, '--coll=player', '--keys=areaId'];

            return P.try(function(){
                // drop index
                return env.runScript(script, ['drop'].concat(args));
            })
            .then(function(){
                // rebuild index
                return env.runScript(script, ['rebuild'].concat(args));
            });
        })
        .then(function(){
            return env.startCluster(shardId);
        })
        .then(function(){
            return memdb.autoConnect(env.config)
            .then(function(ret){
                autoconn = ret;
            });
        })
        .then(function(){
            var Player = autoconn.collection('player');
            return autoconn.transaction(function(){
                return Player.find({areaId : 'a1'})
                .then(function(ret){
                    ret.length.should.eql(1);
                    ret[0]._id.should.eql('p1');
                });
            }, shardId);
        })
        .then(function(){
            return autoconn.close();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });
});
