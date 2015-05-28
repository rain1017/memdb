'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var mongodb = P.promisifyAll(require('mongodb'));

var MongoBackend = function(opts){
    opts = opts || {};

    this.config = {
        url : opts.url || 'mongodb://localhost/test',
        options : opts.options || {},
    };
    this.conn = null;

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + opts.shardId);
};

var proto = MongoBackend.prototype;

proto.start = function(){
    return P.bind(this)
    .then(function(){
        return P.promisify(mongodb.MongoClient.connect)(this.config.url, this.config.options);
    })
    .then(function(ret){
        this.conn = ret;
        this.logger.info('backend mongodb connected to %s', this.config.url);
    });
};

proto.stop = function(){
    return P.bind(this)
    .then(function(){
        return this.conn.closeAsync();
    })
    .then(function(){
        this.logger.info('backend mongodb closed');
    });
};

proto.get = function(name, id){
    this.logger.debug('backend mongodb get(%s, %s)', name, id);

    return this.conn.collection(name).findOneAsync({_id : id});
};

// Return an async iterator with .next(cb) signature
proto.getAll = function(name){
    this.logger.debug('backend mongodb getAll(%s)', name);

    return this.conn.collection(name).findAsync();
};

proto.set = function(name, id, doc){
    this.logger.debug('backend mongodb set(%s, %s)', name, id);

    if(!!doc){
        doc._id = id;
        return this.conn.collection(name).updateAsync({_id : id}, doc, {upsert : true});
    }
    else{
        return this.conn.collection(name).removeAsync({_id : id});
    }
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
    this.logger.debug('backend mongodb setMulti');

    var self = this;
    return P.mapLimit(items, function(item){
        return self.set(item.name, item.id, item.doc);
    });
};

// drop table or database
proto.drop = function(name){
    this.logger.debug('backend mongodb drop %s', name);

    if(!!name){
        return this.conn.collection(name).dropAsync()
        .catch(function(e){
            // Ignore ns not found error
            if(e.message.indexOf('ns not found') === -1){
                throw e;
            }
        });
    }
    else{
        return this.conn.dropDatabaseAsync();
    }
};

proto.getCollectionNames = function(){
    return this.conn.collectionsAsync().then(function(collections){
        return collections.map(function(collection){
            return collection.s.name;
        });
    });
};

module.exports = MongoBackend;
