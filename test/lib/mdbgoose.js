'use strict';

var P = require('bluebird');
var util = require('util');
var should = require('should');
var env = require('../env');
var memdb = require('../../lib');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('mdbgoose test', function(){
    beforeEach(env.flushdb);
    after(env.flushdb);

    it('mdbgoose (standalone mode)', function(cb){
        var mdbgoose = memdb.goose;
        var Schema = mdbgoose.Schema;
        var types = mdbgoose.SchemaTypes;

        mdbgoose._autoconn = null;
        delete mdbgoose.connection.models.player;
        var playerSchema = new Schema({
            _id : String,
            areaId : String,
            name : String,
            fullname : {first: String, second: String},
            items : [types.Mixed],
            extra : types.Mixed,
        }, {collection : 'player', versionKey: false});

        var Player = mdbgoose.model('player', playerSchema);

        var serverProcess = null;

        return P.try(function(){
            return env.startServer('s1');
        })
        .then(function(ret){
            serverProcess = ret;
            return mdbgoose.connectAsync({host : env.config.shards.s1.host, port : env.config.shards.s1.port});
        })
        .then(function(){
            // connect to backend mongodb
            return mdbgoose.connectMongoAsync(env.config.shards.s1.backend.url);
        })
        .then(function(){
            return mdbgoose.transaction(function(){
                var player1 = new Player({
                                    _id : 'p1',
                                    areaId: 'a2',
                                    name: 'rain',
                                    fullname: {first: 'first', second: 'second'},
                                    items : [{name : 'item1', type : 1}],
                                    extra: {xx: 'extra val'},
                                });

                var player2 = new Player({
                                    _id : 'p2',
                                    areaId: 'a1',
                                    name: 'snow',
                                    fullname: {first: 'first', second: 'second'},
                                    items : [{name : 'item1', type : 1}],
                                    extra: {},
                                });

                return P.try(function(){
                    return player1.saveAsync();
                })
                .then(function(){
                    return Player.findAsync({_id : 'p1'})
                    .then(function(players){
                        players.length.should.eql(1);
                        players[0]._id.should.eql('p1');
                    });
                })
                .then(function(){
                    return Player.findAsync('p1')
                    .then(function(player){
                        player._id.should.eql('p1');
                    });
                })
                .then(function(){
                    return Player.findAsync('p1', null, {lock : true})
                    .then(function(player){
                        player._id.should.eql('p1');
                    });
                })
                .then(function(){
                    return Player.findOneAsync({_id : 'p1'})
                    .then(function(player){
                        player._id.should.eql('p1');
                    });
                })
                .then(function(){
                    return Player.findByIdAsync('p1')
                    .then(function(player){
                        player._id.should.eql('p1');
                    });
                })
                .then(function(){
                    return Player.findLockedAsync({_id : 'p1'})
                    .then(function(players){
                        players.length.should.eql(1);
                        players[0]._id.should.eql('p1');
                    });
                })
                .then(function(){
                    return Player.findOneLockedAsync({_id : 'p1'})
                    .then(function(player){
                        player._id.should.eql('p1');
                    });
                })
                .then(function(){
                    return Player.findByIdLockedAsync('p1');
                })
                .then(function(player){
                    logger.debug('%j', player);
                    player.name.should.eql('rain');

                    player.name = 'snow';
                    player.areaId = 'a1';
                    player.fullname.first = 'changed first';
                    player.fullname.second = 'changed second';
                    player.items.push({name : 'item2', type : 2});
                    player.extra = {xx1 : 'changed extra'};

                    return player.saveAsync();
                })
                .then(function(){
                    return P.try(function(){
                        return Player.findAsync('p1');
                    })
                    .then(function(player){
                        logger.debug('%j', player);
                        player.name.should.eql('snow');
                        player.items.length.should.eql(2);
                    });
                })
                .then(function(){
                    return player2.saveAsync();
                })
                .then(function(){
                    return P.try(function(){
                        return Player.findAsync({areaId : 'a1'});
                    })
                    .then(function(players){
                        logger.debug('%j', players);
                        players.length.should.eql(2);
                        players.forEach(function(player){
                            player.areaId.should.eql('a1');
                        });
                    });
                })
                .then(function(){
                    return P.try(function(){
                        return Player.findAsync({areaId : 'a1'}, null, {limit : 1});
                    })
                    .then(function(players){
                        logger.debug('%j', players);
                        players.length.should.eql(1);
                    });
                })
                .then(function(){
                    return Player.findCachedAsync('p1')
                    .then(function(ret){
                        logger.debug('%j', ret);
                    });
                });
            });
        })
        .then(function(){
            return mdbgoose.transaction(function(){
                return mdbgoose.autoconn.flushBackend();
            });
        })
        .then(function(){
            // Call mongodb directly
            return Player.findMongoAsync();
        })
        .then(function(players){
            logger.debug('%j', players);
            players.length.should.eql(2);

            return players[0].saveAsync()
            .catch(function(e){
                logger.warn(e); // should throw error
            });
        })
        .then(function(){
            return memdb.close();
        })
        .finally(function(){
            return env.stopServer(serverProcess);
        })
        .nodeify(cb);
    });

    it('mdbgoose', function(cb){
        var mdbgoose = memdb.goose;
        var Schema = mdbgoose.Schema;
        var types = mdbgoose.SchemaTypes;

        mdbgoose._autoconn = null;
        delete mdbgoose.connection.models.player;
        var playerSchema = new Schema({
            _id : String,
            areaId : String,
            name : String,
            fullname : {first: String, second: String},
            extra : types.Mixed,
        }, {collection : 'player', versionKey: false});

        var Player = mdbgoose.model('player', playerSchema);

        return P.try(function(){
            return memdb.startServer(env.dbConfig('s1'));
        })
        .then(function(){
            return mdbgoose.connectAsync();
        })
        .then(function(){
            return mdbgoose.transaction(function(){
                var player1 = new Player({
                                    _id : 'p1',
                                    areaId: 'a2',
                                    name: 'rain',
                                });
                return P.try(function(){
                    return player1.saveAsync();
                })
                .then(function(){
                    return Player.findAsync('p1');
                })
                .then(function(player){
                    player.name.should.eql('rain');
                    return player.removeAsync();
                });
            });
        })
        .finally(function(){
            return memdb.stopServer();
        })
        .nodeify(cb);
    });

    it('gen collection config', function(cb){
        var mdbgoose = memdb.goose;
        var Schema = mdbgoose.Schema;
        var types = mdbgoose.SchemaTypes;

        mdbgoose._autoconn = null;
        delete mdbgoose.connection.models.player;
        var DummySchema = new Schema({
            _id : String,
            name : String,
            first : {type : String, indexIgnore : ['']},
            last : {type : String, indexIgnore : ['']},
            groupId : {type : String, index : true, indexIgnore : [-1]},
            uniqKey : {type : String, unique : true},
            uniqKey2 : {type : String, index : {unique : true}},
        },  {collection : 'dummy'});

        DummySchema.index({first : 1, last : 1}, {unique : true});

        mdbgoose.model('Dummy', DummySchema);

        var config = mdbgoose.genCollectionConfig();

        var expected = {
            dummy: {
                indexes: [
                {
                    keys: ['groupId'],
                    valueIgnore: {
                        groupId: [-1]
                    }
                },
                {
                    keys: ['uniqKey'],
                    unique: true,
                    valueIgnore: {
                        uniqKey: []
                    }
                },
                {
                    keys: ['uniqKey2'],
                    unique: true,
                    valueIgnore: {
                        uniqKey2: []
                    }
                }, {
                    keys: ['first', 'last'],
                    valueIgnore: {
                        first: [''],
                        last: ['']
                    },
                    unique: true
                }]
            }
        };

        config.dummy.should.eql(expected.dummy);

        cb();
    });
});
