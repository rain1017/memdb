'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var util = require('util');
var utils = require('./utils');
var EventEmitter = require('events').EventEmitter;

var MAX_INDEX_COLLISION = 10000;

/**
 * opts.config = {
 *  indexes : {
 *   '["key1"]' : indexOptions,
 *   '["key2", "key3"]' : indexOptions,
 *  }
 * }
 */
var Collection = function(opts){
    opts = opts || {};
    var self = this;

    this.name = opts.name;
    this.shard = opts.shard;
    this.conn = opts.conn;
    this.config = opts.config || {};
    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shard._id);

    this.pendingIndexTasks = {}; //{id, [Promise]}

    this.shard.on('updateIndex:' + this.name + ':' + this.conn._id, function(id, indexKey, oldValue, newValue){

        if(!self.config.indexes[indexKey]){
            return;
        }

        if(!self.pendingIndexTasks[id]){
            self.pendingIndexTasks[id] = [];
        }
        if(oldValue !== null){
            self.pendingIndexTasks[id].push(self._removeIndex(id, indexKey, oldValue));
        }
        if(newValue !== null){
            self.pendingIndexTasks[id].push(self._insertIndex(id, indexKey, newValue));
        }
    });

    EventEmitter.call(this);
};

util.inherits(Collection, EventEmitter);

var proto = Collection.prototype;

proto.close = function(){
    this.shard.removeAllListeners('updateIndex:' + this.name + ':' + this.conn._id);
};

proto.insert = function(docs){
    if(!Array.isArray(docs)){
        return this._insertById(docs._id, docs);
    }

    var self = this;
    return P.mapSeries(docs, function(doc){ //disable concurrent to avoid race condition
        if(!doc){
            throw new Error('doc is null');
        }
        return self._insertById(doc._id, doc);
    });
};

proto._insertById = function(id, doc){
    if(!utils.isDict(doc)){
        throw new Error('doc must be a dictionary');
    }

    if(id === null || id === undefined){
        id = utils.uuid();
    }
    id = this._checkId(id);
    doc._id = id;

    var self = this;
    return P.try(function(){
        return self.lock(id);
    })
    .then(function(){
        return self.shard.insert(self.conn._id, self._key(id), doc);
    })
    .then(function(){
        return self._finishIndexTasks(id);
    })
    .thenReturn(id);
};

proto.find = function(query, fields, opts){
    if(typeof(query) === 'number' || typeof(query) === 'string'){
        return this.findById(query, fields, opts);
    }

    if(query === null || typeof(query) !== 'object'){
        throw new Error('invalid query');
    }

    if(query.hasOwnProperty('_id')){
        return this.findById(query._id, fields, opts)
        .then(function(doc){
            return [doc];
        });
    }

    var keys = Object.keys(query).sort();
    var values = keys.map(function(key){
        return query[key];
    });

    return this._findByIndex(JSON.stringify(keys), JSON.stringify(values), fields, opts);
};

proto.findOne = function(query, fields, opts){
    opts = opts || {};
    opts.limit = 1;
    return this.find(query, fields, opts)
    .then(function(docs){
        if(docs.length === 0){
            return null;
        }
        return docs[0];
    });
};

proto.findById = function(id, fields, opts){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        if(opts && opts.lock){
            return self.lock(id);
        }
    })
    .then(function(){
        return self.shard.find(self.conn._id, self._key(id), fields, opts);
    });
};

proto.findLocked = function(query, fields, opts){
    opts = opts || {};
    opts.lock = true;
    return this.find(query, fields, opts);
};

proto.findOneLocked = function(query, fields, opts){
    opts = opts || {};
    opts.lock = true;
    return this.findOne(query, fields, opts);
};

proto.findByIdLocked = function(id, fields, opts){
    opts = opts || {};
    opts.lock = true;
    return this.findById(id, fields, opts);
};

proto.findCached = function(id){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        return self.shard.findCached(self.conn._id, self._key(id));
    });
};

// value is object
proto._findByIndex = function(indexKey, indexValue, fields, opts){
    if(!this.config.indexes[indexKey]){
        throw new Error('You must create index for ' + indexKey);
    }

    var indexCollection = this.conn.getCollection(this._indexCollectionName(indexKey));

    var self = this;
    return P.try(function(){
        return indexCollection.findById(indexValue, 'ids', opts);
    })
    .then(function(doc){
        var ids = doc ? Object.keys(doc.ids) : [];
        if(opts && opts.limit){
            ids = ids.slice(0, opts.limit);
        }
        return P.mapSeries(ids, function(id64){
            var id = new Buffer(id64, 'base64').toString();
            return self.findById(id, fields, opts);
        });
    });
};

proto.update = function(query, modifier, opts){
    opts = opts || {};
    var self = this;
    return P.try(function(){
        return self.find(query, '_id', {lock : true});
    })
    .then(function(ret){
        if(!ret || ret.length === 0){
            if(!opts.upsert){
                return 0;
            }
            // upsert
            if(typeof(query) === 'string' || typeof(query) === 'number'){
                query = {_id : query};
            }
            return self.insert(query)
            .then(function(id){
                return self._updateById(id, modifier, opts);
            })
            .then(function(){
                return 1;
            });
        }

        if(!Array.isArray(ret)){
            return self._updateById(ret._id, modifier, opts)
            .then(function(){
                return 1;
            });
        }
        return P.each(ret, function(doc){
            return self._updateById(doc._id, modifier, opts);
        })
        .then(function(){
            return ret.length;
        });
    });
};

proto._updateById = function(id, modifier, opts){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        return self.shard.update(self.conn._id, self._key(id), modifier, opts);
    })
    .then(function(){
        return self._finishIndexTasks(id);
    });
};

proto.remove = function(query, opts){
    var self = this;
    return P.try(function(){
        return self.find(query, '_id', {lock : true});
    })
    .then(function(ret){
        if(!ret){
            return 0;
        }
        if(!Array.isArray(ret)){
            return self._removeById(ret._id, opts)
            .then(function(){
                return 1;
            });
        }
        return P.each(ret, function(doc){
            return self._removeById(doc._id, opts);
        })
        .then(function(){
            return ret.length;
        });
    });
};

proto._removeById = function(id, opts){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        return self.shard.remove(self.conn._id, self._key(id), opts);
    })
    .then(function(){
        return self._finishIndexTasks(id);
    });
};

proto.lock = function(id){
    id = this._checkId(id);

    if(this.shard.isLocked(this.conn._id, this._key(id))){
        return;
    }

    var self = this;
    return P.try(function(){
        return self.shard.lock(self.conn._id, self._key(id));
    })
    .then(function(ret){
        self.emit('lock', id);
        return ret;
    });
};

// value is json encoded
proto._insertIndex = function(id, indexKey, indexValue){
    var indexCollection = this.conn.getCollection(this._indexCollectionName(indexKey));
    var id64 = new Buffer(id).toString('base64');

    if(this.config.indexes[indexKey].unique){
        var doc = {_id : indexValue, 'ids' : {}, 'count' : 1};
        doc.ids[id64] = 1;

        return indexCollection.insert(doc)
        .catch(function(err){
            if(err.message === 'doc already exists'){
                throw new Error('Duplicate value for unique key ' + indexKey);
            }
            throw err;
        });
    }

    var param = {
        $set : {},
        $inc : {count : 1},
    };
    param.$set['ids.' + id64] = 1;

    return P.try(function(){
        return indexCollection.find(indexValue, 'count');
    })
    .then(function(ret){
        if(ret && ret.count >= MAX_INDEX_COLLISION){
            throw new Error('Too many duplicate values on same index');
        }
        return indexCollection.update(indexValue, param, {upsert : true});
    });
};

// value is json encoded
proto._removeIndex = function(id, indexKey, indexValue){
    var indexCollection = this.conn.getCollection(this._indexCollectionName(indexKey));
    var id64 = new Buffer(id).toString('base64');

    return P.try(function(){
        var param = {
            $unset : {},
            $inc: {count : -1}
        };
        param.$unset['ids.' + id64] = 1;
        return indexCollection.update(indexValue, param);
    })
    .then(function(){
        return indexCollection.find(indexValue, 'count');
    })
    .then(function(ret){
        if(ret.count === 0){
            return indexCollection.remove(indexValue);
        }
    });
};

proto._finishIndexTasks = function(id){
    if(!this.pendingIndexTasks[id]){
        return;
    }
    // Save domain
    var d = process.domain;
    var self = this;
    return P.each(self.pendingIndexTasks[id], function(promise){
        return promise;
    })
    .then(function(){
        delete self.pendingIndexTasks[id];
        // Restore domain
        process.domain = d;
    });
};

proto._indexCollectionName = function(indexKey){
    //'__index.name.key1.key2'
    return '__index.' + this.name + '.' + JSON.parse(indexKey).join('.');
};

proto._key = function(id){
    return this.name + ':' + id;
};

proto._checkId = function(id){
    if(typeof(id) === 'number'){
        return id.toString();
    }
    else if(typeof(id) === 'string'){
        return id;
    }
    throw new Error('id must be number or string');
};

module.exports = Collection;
