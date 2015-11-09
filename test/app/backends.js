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
var backends = require('../../app/backends');
var env = require('../env');

describe('backends test', function(){
    beforeEach(env.flushdb);

    var testFunc = function(backend){
        var item1 = {name : 'test', id : 1, doc : {_id : 1, k : 1}};
        var item2 = {name : 'test2', id : 1, doc : {_id : 1, k2 : 1}};

        return P.try(function(){
            return backend.start();
        })
        .then(function(){
            return backend.setMulti([item1, item2]);
        })
        .then(function(){
            return backend.get(item1.name, item1.id)
            .then(function(ret){
                ret.should.eql(item1.doc);
            });
        })
        .then(function(){
            return backend.get(item2.name, item2.id)
            .then(function(ret){
                ret.should.eql(item2.doc);
            });
        })
        .then(function(){
            // update item1
            item1.doc.k = 2;
            // remove item2
            item2.doc = null;
            return backend.setMulti([item1, item2]);
        })
        .then(function(){
            return backend.get(item1.name, item1.id)
            .then(function(ret){
                ret.should.eql(item1.doc);
            });
        })
        .then(function(){
            return backend.get(item2.name, item2.id)
            .then(function(ret){
                (ret === null).should.be.true; // jshint ignore:line
            });
        })
        .then(function(){
            return backend.drop();
        })
        .finally(function(){
            return backend.stop();
        });
    };

    it('mongo backend', function(cb){
        var opts = {
            engine : 'mongodb',
            url : env.config.backend.url,
            shardId : 's1',
        };
        var backend = backends.create(opts);
        return testFunc(backend)
        .nodeify(cb);
    });

    it('redis backend', function(cb){
        var opts = {
            engine : 'redis',
            host : env.config.locking.host,
            port : env.config.locking.port,
            db : env.config.locking.db,
            shardId : 's1',
        };
        var backend = backends.create(opts);
        return testFunc(backend)
        .nodeify(cb);
    });
});

