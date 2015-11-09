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
