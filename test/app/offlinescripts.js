'use strict';

var P = require('bluebird');
var util = require('util');
var should = require('should');
var env = require('../env');
var memdb = require('../../lib');
var offlinescripts = require('../../app/offlinescripts');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('offlinescripts test', function(){
    beforeEach(env.flushdb);
    after(env.flushdb);

    it('rebuildIndex', function(cb){
        var shardId = 's1';
        var autoconn = null;

        return env.startCluster(shardId)
        .then(function(){
            return memdb.autoConnect(env.config)
            .then(function(ret){
                autoconn = ret;
            });
        })
        .then(function(){
            var Dummy = autoconn.collection('dummy');
            return autoconn.transaction(function(){
                return P.try(function(){
                    return Dummy.insert({_id : '1', guid : 'g1'});
                })
                .then(function(){
                    return Dummy.insert({_id : '2', guid : 'g2'});
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
            return offlinescripts.rebuildIndex(env.config.backend, 'dummy', 'guid', {unique : true});
        })
        .then(function(){
            return env.startCluster(shardId, function(config){
                config.collections.dummy = {
                    indexes : [{keys : ['guid'], unique : true}]
                };
            });
        })
        .then(function(){
            return memdb.autoConnect(env.config)
            .then(function(ret){
                autoconn = ret;
            });
        })
        .then(function(){
            var Dummy = autoconn.collection('dummy');
            return autoconn.transaction(function(){
                return Dummy.find({guid : 'g1'})
                .then(function(ret){
                    ret.length.should.eql(1);
                    ret[0]._id.should.eql('1');
                });
            }, shardId);
        })
        .then(function(){
            return autoconn.close();
        })
        .then(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });
});
