'use strict';

var P = require('bluebird');
var Connection = require('./connection');
var AutoConnection = require('./autoconnection');

exports.connect = function(opts){
    var conn = new Connection(opts);
    return P.try(function(){
        return conn.connect();
    })
    .thenReturn(conn);
};

exports.autoConnect = function(opts){
    var conn = new AutoConnection(opts);
    return P.resolve(conn);
};

exports.connectBackend = function(backend){
    var mongodb = P.promisifyAll(require('mongodb'));
    return P.promisify(mongodb.MongoClient.connect)(backend.url, backend.options)
    .then(function(db){
        var getCollection = db.collection;
        db.collection = function(){
            var coll = getCollection.apply(db, arguments);
            var disabledMethods = ['createIndex', 'drop', 'dropIndex', 'dropIndexes', 'ensureIndex',
            'findAndModify', 'getCollection', 'getDB', 'insert', 'remove', 'renameCollection', 'save', 'update'];
            disabledMethods.forEach(function(method){
                coll[method] = function(){
                    throw new Error('write to backend is forbidden');
                };
            });
            return coll;
        };
        return db;
    });
};

Object.defineProperty(exports, 'goose', {
    get : function(){
        return require('./mdbgoose');
    },
});

Object.defineProperty(exports, 'logger', {
    get : function(){
        return require('memdb-logger');
    },
});

Object.defineProperty(exports, 'Promise', {
    get : function(){
        return require('bluebird');
    },
});
