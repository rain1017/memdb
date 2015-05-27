'use strict';

var P = require('bluebird');
var util = require('util');
var should = require('should');
var env = require('../env');
var Slave = require('../../app/slave');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('slave test', function(){
    beforeEach(env.flushdb);
    after(env.flushdb);

    it('set/del', function(cb){
        var shard = {_id : 's1'};
        var slave = new Slave(shard, env.config.shards.s1.slave);

        var key1 = 'player$1';
        var doc1 = {name : 'rain', age : 30};
        var key2 = 'player$2';
        var doc2 = null;
        var changes = {
            'player$1' : {name : 'snow', age : undefined},
            'player$2' : {name : 'tina'},
        };

        return P.try(function(){
            return slave.start();
        })
        .then(function(){
            return slave.set(key1, doc1);
        })
        .then(function(){
            return slave.set(key2, doc2);
        })
        .then(function(){
            return slave.findMulti([key1, key2])
            .then(function(docs){
                docs[key1].should.eql(doc1);
                (docs[key2] === null).should.eql(true);
            });
        })
        .then(function(){
            return slave.setMulti(changes);
        })
        .then(function(){
            return slave.findMulti([key1, key2])
            .then(function(docs){
                docs[key1].should.eql({name : 'snow'});
                docs[key2].should.eql({name : 'tina'});
            });
        })
        .then(function(){
            return slave.del(key2);
        })
        .then(function(){
            return slave.del(key1);
        })
        .then(function(){
            return slave.findMulti([key1, key2])
            .then(function(docs){
                Object.keys(docs).length.should.eql(0);
            });
        })
        .then(function(){
            return slave.stop();
        })
        .nodeify(cb);
    });

    it('getAll/clear', function(cb){
        var shard = {_id : 's1'};
        var slave = new Slave(shard, env.config.shards.s1.slave);

        var key1 = 'player$1';
        var doc1 = {name : 'rain', age : 30};
        var key2 = 'player$2';
        var doc2 = null;

        return P.try(function(){
            return slave.start();
        })
        .then(function(){
            return slave.set(key1, doc1);
        })
        .then(function(){
            return slave.set(key2, doc2);
        })
        .then(function(){
            return slave.getAllKeys()
            .then(function(keys){
                keys.length.should.eql(2);
            });
        })
        .then(function(){
            return slave.clear();
        })
        .then(function(){
            return slave.getAllKeys()
            .then(function(keys){
                keys.length.should.eql(0);
            });
        })
        .nodeify(cb);
    });
});
