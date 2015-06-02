'use strict';

var P = require('bluebird');
var Connection = require('./connection');
var AutoConnection = require('./autoconnection');

var _db = null;

// Start in-process db server
exports.startServer = function(opts){
    if(_db){
        throw new Error('server already started');
    }

    var Database = require('../app/database');
    _db = new Database(opts);
    return _db.start();
};

// Stop in-process db server
exports.stopServer = function(){
    if(!_db){
        throw new Error('please startServer first');
    }

    return _db.stop()
    .then(function(){
        _db = null;
    });
};

exports.connect = function(opts){
    opts = opts || {};
    if(!(opts.host && opts.port)){
        if(!_db){
            throw new Error('please startServer first');
        }
        opts.db = _db;
    }

    var conn = new Connection(opts);
    return P.try(function(){
        return conn.connect();
    })
    .thenReturn(conn);
};

exports.autoConnect = function(opts){
    opts = opts || {};
    if(!opts.shards){
        if(!_db){
            throw new Error('please startServer first');
        }
        opts.db = _db;
    }

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

Object.defineProperty(exports, 'offlinescripts', {
    get : function(){
        return require('../app/offlinescripts');
    },
});
