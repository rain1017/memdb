'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var consts = require('./consts');
var Collection = require('./collection');

var Connection = function(opts){
    opts = opts || {};

    this.config = opts.config;

    this._id = opts._id;
    this.shard = opts.shard;
    this.collections = {};

    this.lockedKeys = {};
    this._dirty = false;

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shard._id);
};

var proto = Connection.prototype;

proto.close = function(){
    if(this._dirty){
        this.rollback();
    }

    for(var name in this.collections){
        this.collections[name].close();
    }
};

consts.collMethods.forEach(function(method){
    proto[method] = function(name){
        var collection = this.getCollection(name);

        // remove 'name' arg
        var args = [].slice.call(arguments, 1);
        this.logger.debug('[conn:%s] %s.%s(%j)', this._id, name, method, args);

        return collection[method].apply(collection, args);
    };
});

proto.commit = function(){
    var self = this;
    return P.try(function(){
        return self.shard.commit(self._id, Object.keys(self.lockedKeys));
    })
    .then(function(){
        self.lockedKeys = {};
        self._dirty = false;

        self.logger.debug('[conn:%s] commited', self._id);
    });
};

proto.rollback = function(){
    this.shard.rollback(this._id, Object.keys(this.lockedKeys));

    this.lockedKeys = {};
    this._dirty = false;

    this.logger.debug('[conn:%s] rolledback', this._id);
};

proto.flushBackend = function(){
    return this.shard.flushBackend(this._id);
};

proto.getCollection = function(name){
    var self = this;
    if(!this.collections[name]){
        var collection = new Collection({
            name : name,
            shard : this.shard,
            conn : this,
            config : this.config.collections[name] || {},
            logger : this.logger,
        });
        collection.on('lock', function(id){
            self._dirty = true;
            var key = name + ':' + id;
            self.lockedKeys[key] = true;
        });
        this.collections[name] = collection;
    }
    return this.collections[name];
};

module.exports = Connection;
