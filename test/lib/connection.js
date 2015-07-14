'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var memdb = require('../../lib');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('connection test', function(){
    beforeEach(env.flushdb);

    it('find/update/insert/remove/commit/rollback', function(cb){
        var conn = null;
        var User = null, News = null;
        var user1 = {_id : '1', name : 'rain', age : 30};
        var user2 = {_id : '2', name : 'tina', age : 24};
        var news1 = {_id : '1', text : 'hello'};

        return P.try(function(){
            return env.startCluster('s1');
        })
        .then(function(){
            return memdb.connect(env.config.shards.s1)
            .then(function(ret){
                conn = ret;
            });
        })
        .then(function(){
            User = conn.collection('user');
            News = conn.collection('news');

            return User.insert([user1, user2]);
        })
        .then(function(ret){
            return conn.commit();
        })
        .then(function(){
            return User.update(user1._id, {$set : {age : 31}});
        })
        .then(function(){
            return User.find(user1._id, 'age')
            .then(function(ret){
                ret.age.should.eql(31);
            });
        })
        .then(function(){
            return User.find(user2._id)
            .then(function(ret){
                ret.should.eql(user2);
            });
        })
        .then(function(){
            return User.remove(user2._id);
        })
        .then(function(){
            return News.insert(news1);
        })
        .then(function(){
            return User.find(user2._id)
            .then(function(ret){
                (ret === null).should.eql(true);
            });
        })
        .then(function(){
            return conn.rollback();
        })
        .then(function(){
            return User.findOne({_id : user1._id})
            .then(function(ret){
                ret.should.eql(user1);
            });
        })
        .then(function(){
            return User.findOne({_id : user2._id})
            .then(function(ret){
                ret.should.eql(user2);
            });
        })
        .then(function(){
            return News.findOne({_id : news1._id})
            .then(function(ret){
                (ret === null).should.eql(true);
            });
        })
        .then(function(){
            return User.remove(user1._id);
        })
        .then(function(){
            return User.remove(user2._id);
        })
        .then(function(){
            return conn.commit();
        })
        .then(function(){
            return conn.close();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });

    it('index test', function(cb){
        var conn = null;
        var Player = null;
        var errCount = 0;

        return P.try(function(){
            return env.startCluster('s1');
        })
        .then(function(){
            return memdb.connect(env.config.shards.s1)
            .then(function(ret){
                conn = ret;
            });
        })
        .then(function(){
            Player = conn.collection('player');

            return Player.find({areaId : 1})
            .then(function(players){
                players.length.should.eql(0);
            });
        })
        .then(function(){
            return Player.insert([{_id : '1', areaId : 1}, {_id : '2', areaId : 1}]);
        })
        .then(function(){
            return Player.find({areaId : 1})
            .then(function(players){
                players.length.should.eql(2);
                players.forEach(function(player){
                    player.areaId.should.eql(1);
                });
            });
        })
        .then(function(){
            return Player.update({areaId : 1}, {$set : {areaId : 2}});
        })
        .then(function(){
            return Player.find({areaId : 1})
            .then(function(players){
                players.length.should.eql(0);
            });
        })
        .then(function(){
            return Player.find({areaId : 2})
            .then(function(players){
                players.length.should.eql(2);
            });
        })
        .then(function(){
            return Player.remove({areaId : 2});
        })
        .then(function(){
            return Player.find({areaId : 2})
            .then(function(players){
                players.length.should.eql(0);
            });
        })
        .then(function(){
            // Should ignore the index
            return Player.insert({_id : '1', areaId : -1});
        })
        .then(function(){
            // Should ignore the index
            return Player.find({areaId : -1})
            .catch(function(err){
                errCount++;
            });
        })
        .then(function(){
            // invalid index value
            return Player.update('1', {$set : {areaId : ['invalid value']}}, {upsert : true})
            .catch(function(err){
                errCount++;
            });
        })
        .then(function(){
            errCount.should.eql(2);

            return conn.close();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });

    it('compound index test', function(cb){
        var conn = null;
        var Player = null;

        return P.try(function(){
            return env.startCluster('s1');
        })
        .then(function(){
            return memdb.connect(env.config.shards.s1)
            .then(function(ret){
                conn = ret;
            });
        })
        .then(function(){
            Player = conn.collection('player');

            return Player.insert({_id : '1', deviceType : 1, deviceId : 'id1'});
        })
        .then(function(){
            return Player.find({deviceType : 1, deviceId : 'id1'})
            .then(function(players){
                players.length.should.eql(1);
            });
        })
        .then(function(){
            return Player.update({_id : '1'}, {$set : {deviceType : 2}});
        })
        .then(function(){
            return Player.find({deviceType : 2, deviceId : 'id1'})
            .then(function(players){
                players.length.should.eql(1);
            });
        })
        .then(function(){
            return Player.insert({_id : '2', deviceType : 2, deviceId : 'id1'})
            .catch(function(err){
                // Should throw duplicate key error
                logger.debug(err);
            });
        })
        .then(function(){
            // rolledback on previous error
            return Player.find({deviceType : 2, deviceId : 'id1'})
            .then(function(players){
                players.length.should.eql(0);
            });
        })
        .then(function(){
            return conn.close();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });

    it('concurrent query on same connection', function(cb){
        var conn = null;
        var errCount = 0;

        return P.try(function(){
            return env.startCluster('s1');
        })
        .then(function(){
            return memdb.connect(env.config.shards.s1)
            .then(function(ret){
                conn = ret;
            });
        })
        .then(function(){
            var Player = conn.collection('player');
            Player.insert({_id : 1, name : 'rain'});
            Player.remove({_id : 1})
            .catch(function(e){
                errCount++;
            });
        })
        .delay(200)
        .then(function(){
            return conn.commit();
        })
        .then(function(){
            errCount.should.eql(1);
            return conn.close();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });

    it('eval', function(cb){
        var conn = null;

        return P.try(function(){
            return env.startCluster('s1');
        })
        .then(function(){
            return memdb.connect(env.config.shards.s1)
            .then(function(ret){
                conn = ret;
            });
        })
        .then(function(){
            var script = '\
            P.try(function(){\
                return db.insert("test", doc);\
            })\
            .then(function(){\
                return db.commit();\
            });';

            var context = {
                doc : {_id : '1', name :'rain'},
            };
            return conn.eval(script, context); //jshint ignore:line
        })
        .then(function(){
            return conn.collection('test').find('1')
            .then(function(ret){
                ret.name.should.eql('rain');
            });
        })
        .then(function(){
            return conn.collection('test').remove('1');
        })
        .then(function(){
            return conn.commit();
        })
        .then(function(){
            return conn.close();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });

    it('collection/field name restriction', function(cb){
        var conn = null;
        var Collection = null;
        var errCount = 0;

        return P.try(function(){
            return env.startCluster('s1');
        })
        .then(function(){
            return memdb.connect(env.config.shards.s1)
            .then(function(ret){
                conn = ret;
            });
        })
        .then(function(){
            Collection = conn.collection('invalid$name');
            return Collection.insert({_id : 1, name : 'rain'});
        })
        .catch(function(err){
            errCount++;
        })
        .then(function(){
            Collection = conn.collection('this is\\valid.coll:name');
            return Collection.insert({_id : 1, name : 'this.is:valid$value'});
        })
        .then(function(){
            return Collection.insert({_id : 2, 'invalid.field' : 'value'});
        })
        .catch(function(err){
            errCount++;
        })
        .then(function(){
            return Collection.insert({_id : 3, '$invalidfield' : 'value'});
        })
        .catch(function(err){
            errCount++;
        })
        .then(function(){
            errCount.should.eql(3);

            return conn.close();
        })
        .finally(function(){
            return env.stopCluster();
        })
        .nodeify(cb);
    });
});
