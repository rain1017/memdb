'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var Database = require('../../app/database');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('database test', function(){
    beforeEach(env.flushdb);

    // it('find/update/insert/remove/commit/rollback', function(cb){
    //  //tested in ../lib/connection
    // });

    // it('index test', function(cb){
    //  //tested in ../lib/connection
    // });

    it('persistent / idle timeout', function(cb){
        var config = env.shardConfig('s1');
        config.persistentDelay = 50;
        config.idleTimeout = 500;
        var db = new Database(config);

        var conn = null;

        var collName = 'player', doc = {_id : '1', name : 'rain'};
        return P.try(function(){
            return db.start();
        })
        .then(function(){
            conn = db.getConnection(db.connect().connId);

            return conn.insert(collName, doc);
        })
        .then(function(){
            return conn.commit();
        })
        .delay(300) // doc persistented
        .then(function(){
            // read from backend
            return db.shard.backend.get(collName, doc._id)
            .then(function(ret){
                ret.should.eql(doc);
            });
        })
        .delay(500) // doc idle timed out
        .then(function(){
            db.shard._isLoaded(collName + '$' + doc._id).should.eql(false);
        })
        .then(function(){
            return conn.remove(collName, doc._id);
        })
        .then(function(){
            return conn.commit();
        })
        .then(function(){
            return db.stop();
        })
        .nodeify(cb);
    });

    it('restore from slave', function(cb){
        var db1 = null, db2 = null;
        var conn = null;
        var player1 = {_id : 'p1', name : 'rain', age: 30};
        var player2 = {_id : 'p2', name : 'snow', age: 25};

        return P.try(function(){
            var config = env.shardConfig('s1');
            config.heartbeatInterval = -1; // disable heartbeat
            config.gcInterval = 3600 * 1000; // disable gc
            db1 = new Database(config);
            return db1.start();
        })
        .then(function(){
            conn = db1.getConnection(db1.connect().connId);
        })
        .then(function(){
            return conn.insert('player', player1);
        })
        .then(function(){
            return conn.insert('player', player2);
        })
        .then(function(){
            return conn.commit();
        })
        .then(function(){
            db1.shard.state = 4; // Db is suddenly stopped
        })
        .then(function(){
            //restart db
            db2 = new Database(env.shardConfig('s1'));
            return db2.start();
        })
        .then(function(){
            conn = db2.getConnection(db2.connect().connId);
        })
        .then(function(){
            return P.try(function(){
                return conn.find('player', player1._id);
            })
            .then(function(ret){
                ret.should.eql(player1);
            });
        })
        .then(function(){
            return P.try(function(){
                return conn.find('player', player2._id);
            })
            .then(function(ret){
                ret.should.eql(player2);
            });
        })
        .then(function(){
            conn.close();

            return db2.stop();
        })
        .finally(function(){
            // clean up
            db1.shard.state = 2;
            return db1.stop(true);
        })
        .nodeify(cb);
    });
});
