'use strict';

var P = require('bluebird');
var util = require('util');
var should = require('should');
var env = require('../env');
var memdb = require('../../lib');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('offlinescripts test', function(){
    beforeEach(env.flushdb);
    after(env.flushdb);

    it('rebuildIndex', function(cb){
        var config = env.dbConfig('s1');
        return P.try(function(){
            return memdb.startServer(config);
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(conn){
            var Player = conn.collection('player');
            return conn.transaction(function(){
                return P.try(function(){
                    return Player.insert({_id : 'p1', guid : 'g1'});
                })
                .then(function(){
                    return Player.insert({_id : 'p2', guid : 'g2'});
                });
            });
        })
        .then(function(){
            return memdb.stopServer();
        })
        .then(function(){
            return memdb.offlinescripts.rebuildIndex(config.backend, 'player', 'guid', {unique : true});
        })
        .then(function(){
            config.collections.player.indexes.push({
                keys : ['guid'],
                unique : true,
            });
            return memdb.startServer(config);
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(conn){
            var Player = conn.collection('player');
            return conn.transaction(function(){
                return Player.find({guid : 'g1'})
                .then(function(ret){
                    ret.length.should.eql(1);
                    ret[0]._id.should.eql('p1');
                });
            });
        })
        .then(function(){
            return memdb.stopServer();
        })
        .nodeify(cb);
    });
});
