'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var memdb = require('../../lib');
var env = require('../env');
var logger = require('memdb-logger').getLogger('test', __filename);

describe('autoconnection test', function(){
    beforeEach(env.flushdb);
    after(env.flushdb);

    it('concurrent execute', function(cb){
        var shardId = Object.keys(env.config.shards)[0];
        var user1 = {_id : '1', name : 'rain', level : 0};
        var autoconn = null;

        return P.try(function(){
            return memdb.startServer(env.dbConfig(shardId));
        })
        .then(function(){
            return memdb.autoConnect();
        })
        .then(function(ret){
            autoconn = ret;
            return autoconn.transaction(function(){
                var User = autoconn.collection('user');
                return User.insert(user1);
            });
        })
        .then(function(){
            var count = 8;

            var delay = 0;
            return P.map(_.range(count), function(){
                delay += 10;

                return P.delay(delay)
                .then(function(){
                    // Simulate non-atomic check and update operation
                    // each 'thread' add 1 to user1.level
                    return autoconn.transaction(function(){
                        var User = autoconn.collection('user');
                        var level = null;

                        return P.try(function(){
                            return User.find(user1._id, 'level');
                        })
                        .then(function(ret){
                            level = ret.level;
                        })
                        .delay(20)
                        .then(function(){
                            return User.update(user1._id, {level : level + 1});
                        });
                    });
                });
            })
            .then(function(){
                return autoconn.transaction(function(){
                    var User = autoconn.collection('user');
                    return P.try(function(){
                        return User.find(user1._id);
                    })
                    .then(function(ret){
                        // level should equal to count
                        ret.level.should.eql(count);
                        return User.remove(user1._id);
                    });
                });
            });
        })
        .then(function(){
            return autoconn.transaction(function(){
                return P.try(function(){
                    var User = autoconn.collection('user');
                    return User.insert(user1);
                }).then(function(){
                    //Should roll back on exception
                    throw new Error('Oops!');
                });
            })
            .catch(function(e){
                e.message.should.eql('Oops!');
            });
        })
        .then(function(){
            return autoconn.transaction(function(){
                return P.try(function(){
                    var User = autoconn.collection('user');
                    return User.find(user1._id);
                })
                .then(function(ret){
                    (ret === null).should.eql(true);
                });
            });
        })
        .then(function(){
            return memdb.close();
        })
        .finally(function(){
            return memdb.stopServer();
        })
        .nodeify(cb);
    });
});
