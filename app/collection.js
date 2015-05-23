'use strict';

var P = require('bluebird');
var util = require('util');
var utils = require('./utils');
var EventEmitter = require('events').EventEmitter;
var logger = require('pomelo-logger').getLogger('memdb', __filename);

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
    var self = this;

    this.name = opts.name;
    this.shard = opts.shard;
    this.db = opts.db;
    this.config = opts.config || {};

    this.pendingIndexTasks = {}; //{id, [Promise]}

    this.shard.on('docUpdateIndex:' + this.name, function(connId, id, indexKey, oldValue, newValue){
        if(!self.config.indexes[indexKey]){
            return;
        }

        if(!self.pendingIndexTasks[id]){
            self.pendingIndexTasks[id] = [];
        }
        if(oldValue !== null){
            self.pendingIndexTasks[id].push(self._removeIndex(connId, id, indexKey, oldValue));
        }
        if(newValue !== null){
            self.pendingIndexTasks[id].push(self._insertIndex(connId, id, indexKey, newValue));
        }
    });

    EventEmitter.call(this);
};

util.inherits(Collection, EventEmitter);

var proto = Collection.prototype;

proto.insert = function(connId, docs){
    if(!Array.isArray(docs)){
        return this._insertById(connId, docs._id, docs);
    }

    var self = this;
    return P.mapLimit(docs, function(doc){
        if(!doc){
            throw new Error('doc is null');
        }
        return self._insertById(connId, doc._id, doc);
    }, 1); //disable concurrent to avoid race condition
};

proto._insertById = function(connId, id, doc){
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
        return self.lock(connId, id);
    })
    .then(function(){
        return self.shard.insert(connId, self._key(id), doc);
    })
    .then(function(){
        return self._finishIndexTasks(id);
    })
    .then(function(){
        return id;
    });
};

proto.find = function(connId, query, fields, opts){
    if(typeof(query) === 'number' || typeof(query) === 'string'){
        return this.findById(connId, query, fields, opts);
    }

    if(query === null || typeof(query) !== 'object'){
        throw new Error('invalid query');
    }

    if(query.hasOwnProperty('_id')){
        return this.findById(connId, query._id, fields, opts)
        .then(function(doc){
            return [doc];
        });
    }

    var keys = Object.keys(query).sort();
    var values = keys.map(function(key){
        return query[key];
    });

    return this._findByIndex(connId, JSON.stringify(keys), JSON.stringify(values), fields, opts);
};

proto.findOne = function(connId, query, fields, opts){
    opts = opts || {};
    opts.limit = 1;
    return this.find(connId, query, fields, opts)
    .then(function(docs){
        if(docs.length === 0){
            return null;
        }
        return docs[0];
    });
};

proto.findById = function(connId, id, fields, opts){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        if(opts && opts.lock){
            return self.lock(connId, id);
        }
    })
    .then(function(){
        return self.shard.find(connId, self._key(id), fields, opts);
    });
};

proto.findLocked = function(connId, query, fields, opts){
    opts = opts || {};
    opts.lock = true;
    return this.find(connId, query, fields, opts);
};

proto.findOneLocked = function(connId, query, fields, opts){
    opts = opts || {};
    opts.lock = true;
    return this.findOne(connId, query, fields, opts);
};

proto.findByIdLocked = function(connId, id, fields, opts){
    opts = opts || {};
    opts.lock = true;
    return this.findById(connId, id, fields, opts);
};

proto.findCached = function(connId, id){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        return self.shard.findCached(connId, self._key(id));
    });
};

// value is object
proto._findByIndex = function(connId, indexKey, indexValue, fields, opts){
    if(!this.config.indexes[indexKey]){
        throw new Error('You must create index for ' + indexKey);
    }

    var indexCollection = this.db._collection(this._indexCollectionName(indexKey));

    var self = this;
    return P.try(function(){
        return indexCollection.findById(connId, indexValue, 'ids', opts);
    })
    .then(function(doc){
        var ids = doc ? Object.keys(doc.ids) : [];
        if(opts && opts.limit){
            ids = ids.slice(0, opts.limit);
        }
        return P.mapLimit(ids, function(id64){
            var id = new Buffer(id64, 'base64').toString();
            return self.findById(connId, id, fields, opts);
        }, 1);
    });
};

proto.update = function(connId, query, modifier, opts){
    opts = opts || {};
    var self = this;
    return P.try(function(){
        return self.find(connId, query, '_id', {lock : true});
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
            return self.insert(connId, query)
            .then(function(id){
                return self._updateById(connId, id, modifier, opts);
            })
            .then(function(){
                return 1;
            });
        }

        if(!Array.isArray(ret)){
            return self._updateById(connId, ret._id, modifier, opts)
            .then(function(){
                return 1;
            });
        }
        return P.mapLimit(ret, function(doc){
            return self._updateById(connId, doc._id, modifier, opts);
        }, 1)
        .then(function(){
            return ret.length;
        });
    });
};

proto._updateById = function(connId, id, modifier, opts){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        return self.shard.update(connId, self._key(id), modifier, opts);
    })
    .then(function(){
        return self._finishIndexTasks(id);
    });
};

proto.remove = function(connId, query, opts){
    var self = this;
    return P.try(function(){
        return self.find(connId, query, '_id', {lock : true});
    })
    .then(function(ret){
        if(!ret){
            return 0;
        }
        if(!Array.isArray(ret)){
            return self._removeById(connId, ret._id, opts)
            .then(function(){
                return 1;
            });
        }
        return P.mapLimit(ret, function(doc){
            return self._removeById(connId, doc._id, opts);
        }, 1)
        .then(function(){
            return ret.length;
        });
    });
};

proto._removeById = function(connId, id, opts){
    id = this._checkId(id);

    var self = this;
    return P.try(function(){
        return self.shard.remove(connId, self._key(id), opts);
    })
    .then(function(){
        return self._finishIndexTasks(id);
    });
};

proto.lock = function(connId, id){
    id = this._checkId(id);

    if(this.shard.isLocked(connId, this._key(id))){
        return;
    }

    var self = this;
    return P.try(function(){
        return self.shard.lock(connId, self._key(id));
    })
    .then(function(ret){
        self.emit('lock', connId, id);
        return ret;
    });
};

// value is json encoded
proto._insertIndex = function(connId, id, indexKey, indexValue){
    logger.debug('insertIndex: %s %s %s', id, indexKey, indexValue);

    var indexCollection = this.db._collection(this._indexCollectionName(indexKey));
    var id64 = new Buffer(id).toString('base64');

    if(this.config.indexes[indexKey].unique){
        var doc = {_id : indexValue, 'ids' : {}, 'count' : 1};
        doc.ids[id64] = 1;

        return indexCollection.insert(connId, doc)
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
        return indexCollection.find(connId, indexValue, 'count');
    })
    .then(function(ret){
        if(ret && ret.count >= MAX_INDEX_COLLISION){
            throw new Error('Too many duplicate values on same index');
        }
        return indexCollection.update(connId, indexValue, param, {upsert : true});
    });
};

// value is json encoded
proto._removeIndex = function(connId, id, indexKey, indexValue){
    logger.debug('removeIndex: %s %s %s', id, indexKey, indexValue);

    var indexCollection = this.db._collection(this._indexCollectionName(indexKey));
    var id64 = new Buffer(id).toString('base64');

    return P.try(function(){
        var param = {
            $unset : {},
            $inc: {count : -1}
        };
        param.$unset['ids.' + id64] = 1;
        return indexCollection.update(connId, indexValue, param);
    })
    .then(function(){
        return indexCollection.find(connId, indexValue, 'count');
    })
    .then(function(ret){
        if(ret.count === 0){
            return indexCollection.remove(connId, indexValue);
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
    return P.mapLimit(self.pendingIndexTasks[id], function(promise){
        return promise;
    }, 1)
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
