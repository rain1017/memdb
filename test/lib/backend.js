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
var should = require('should');
var memdb = require('../../lib');
var env = require('../env');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('access backend test', function(){
    it('connect backend', function(cb){
        var db = null, errCount = 0;
        return P.try(function(){
            return memdb.connectBackend(env.config.backend);
        })
        .then(function(ret){
            db = ret;
            // find should success
            return db.collection('player').findOneAsync()
            .then(function(doc){
                logger.info('%j', doc);
            });
        })
        .then(function(){
            return db.collection('player').removeAsync()
            .catch(function(e){
                logger.warn(e.stack);
                // could not write, should throw error;
                errCount++;
            });
        })
        .then(function(){
            errCount.should.eql(1);
        })
        .nodeify(cb);
    });
});
