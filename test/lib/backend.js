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
