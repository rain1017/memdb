'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var consts = require('./consts');
var Collection = require('./collection');
var util = require('util');
var utils = require('./utils');

var Connection = function(opts){
    opts = opts || {};

    this._id = opts._id;
    this.shard = opts.shard;

    this.config = opts.config || {};
    this.collections = {};

    this.lockedKeys = {};

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shard._id);
};

var proto = Connection.prototype;

proto.close = function(){
    if(this.isDirty()){
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
    return P.each(Object.keys(this.collections), function(name){
        var collection = self.collections[name];
        return collection.commitIndex();
    })
    .then(function(){
        return self.shard.commit(self._id, Object.keys(self.lockedKeys));
    })
    .then(function(){
        self.lockedKeys = {};

        self.logger.debug('[conn:%s] commited', self._id);
        return true;
    });
};

// sync method
proto.rollback = function(){
    var self = this;
    Object.keys(this.collections).forEach(function(name){
        self.collections[name].rollbackIndex();
    });

    this.shard.rollback(this._id, Object.keys(this.lockedKeys));
    this.lockedKeys = {};

    this.logger.debug('[conn:%s] rolledback', this._id);
    return true;
};

proto.flushBackend = function(){
    return this.shard.flushBackend(this._id);
};

// for internal use
proto.$unload = function(key){
    return this.shard.$unload(key);
};
// for internal use
proto.$findReadOnly = function(key, fields){
    return this.shard.find(null, key, fields);
};

proto.getCollection = function(name, isIndex){
    if(!isIndex && name && name.indexOf('index.') === 0){
        throw new Error('Collection name can not begin with "index."');
    }

    var self = this;
    if(!this.collections[name]){
        var collection = new Collection({
            name : name,
            shard : this.shard,
            conn : this,
            config : this.config.collections[name] || {},
        });

        collection.on('lock', function(id){
            var key = name + '$' + id;
            self.lockedKeys[key] = true;
        });

        this.collections[name] = collection;
    }
    return this.collections[name];
};


proto.isDirty = function(){
    return Object.keys(this.lockedKeys).length > 0;
};

module.exports = Connection;
