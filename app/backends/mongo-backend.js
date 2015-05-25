'use strict';

var P = require('bluebird');
var mongodb = P.promisifyAll(require('mongodb'));

var MongoBackend = function(opts){
    opts = opts || {};
    this.logger = opts.logger || require('memdb-logger').getLogger('memdb', __filename);

    this._url = opts.url || 'mongodb://localhost/test';
    this._options = opts.options || {};
};

var proto = MongoBackend.prototype;

proto.start = function(){
    return P.bind(this)
    .then(function(){
        return P.promisify(mongodb.MongoClient.connect)(this._url, this._options);
    })
    .then(function(ret){
        this.conn = ret;

        Object.defineProperty(this, 'connection', {
            get : function(){
                return ret;
            }
        });

        this.logger.info('backend mongodb connected to %s', this._url);
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
    this.logger.debug('backend mongodb get %s %s', name, id);
    return this.conn.collection(name).findOneAsync({_id : id});
};

// Return an async iterator with .next(cb) signature
proto.getAll = function(name){
    this.logger.debug('backend mongodb getAll %s', name);
    return this.conn.collection(name).findAsync();
};

proto.set = function(name, id, doc){
    this.logger.debug('backend mongodb set %s %s', name, id);
    if(doc !== null && doc !== undefined){
        doc._id = id;
        return this.conn.collection(name).updateAsync({_id : id}, doc, {upsert : true});
    }
    else{
        return this.conn.collection(name).removeAsync({_id : id});
    }
};

// items : [{name, id, doc}]
proto.setMulti = function(items){
    var self = this;

    this.logger.debug('backend mongodb setMulti');
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
