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
