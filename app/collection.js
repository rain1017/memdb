'use strict';

var P = require('bluebird');
var Logger = require('memdb-logger');
var util = require('util');
var utils = require('./utils');
var EventEmitter = require('events').EventEmitter;

var DEFAULT_MAX_COLLISION = 10000;

var Collection = function(opts){
    opts = opts || {};

    this.name = opts.name;
    this._checkName(this.name);

    this.shard = opts.shard;
    this.conn = opts.conn;
    this.config = opts.config || {};

    this.pendingIndexTasks = {}; //{id, [Promise]}

    var self = this;

    this.onUpdateIndex = function(id, indexKey, oldValue, newValue){
        var config = self.config.indexes[indexKey];
        if(!config){
            return;
        }

        if(!self.pendingIndexTasks[id]){
            self.pendingIndexTasks[id] = [];
        }
        if(oldValue !== null){
            self.pendingIndexTasks[id].push(self._removeIndex(id, indexKey, oldValue, config));
        }
        if(newValue !== null){
            self.pendingIndexTasks[id].push(self._insertIndex(id, indexKey, newValue, config));
        }
    };
    this.updateIndexEvent = 'updateIndex$' + this.name + '$' + this.conn._id;
    this.shard.on(this.updateIndexEvent, this.onUpdateIndex);

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shard._id);

    EventEmitter.call(this);
};

util.inherits(Collection, EventEmitter);

var proto = Collection.prototype;

proto.close = function(){
    this.shard.removeListener(this.updateIndexEvent, this.onUpdateIndex);
};

proto.insert = function(docs){
    if(!Array.isArray(docs)){
        return this._insertById(docs._id, docs);
    }

    var self = this;
    return P.mapSeries(docs, function(doc){ //disable concurrent to avoid race condition
        return self._insertById(doc && doc._id, doc);
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

    if(!utils.isDict(query)){
        throw new Error('invalid query');
    }

    if(query.hasOwnProperty('_id')){
        return this.findById(query._id, fields, opts)
        .then(function(doc){
            if(!doc){
                return [];
            }
            return [doc];
        });
    }

    var keys = Object.keys(query).sort();
    var indexKey = JSON.stringify(keys);

    var indexConfig = this.config.indexes[indexKey];
    if(!indexConfig){
        throw new Error('No index configured for keys - ' + indexKey);
    }

    var valueIgnore = indexConfig.valueIgnore || {};
    var values = keys.map(function(key){
        var value = query[key];
        if(value === null || value === undefined){
            throw new Error('query value can not be null or undefined');
        }
        var ignores = valueIgnore[key] || [];
        if(ignores.indexOf(value) !== -1){
            throw new Error('value ' + value + ' for key ' + key + ' is ignored in index');
        }
        return value;
    });
    var indexValue = JSON.stringify(values);

    return this._findByIndex(indexKey, indexValue, fields, opts);
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

    if(opts && opts.readonly){
        return this.shard.findReadOnly(this.conn._id, this._key(id));
    }

    var self = this;
    return P.try(function(){
        return self.lock(id);
    })
    .then(function(){
        return self.shard.find(self.conn._id, self._key(id), fields, opts);
    });
};

proto.findReadOnly = function(query, fields, opts){
    opts = opts || {};
    opts.readonly = true;
    return this.find(query, fields, opts);
};

proto.findOneReadOnly = function(query, fields, opts){
    opts = opts || {};
    opts.readonly = true;
    return this.findOne(query, fields, opts);
};

proto.findByIdReadOnly = function(id, fields, opts){
    opts = opts || {};
    opts.readonly = true;
    return this.findById(id, fields, opts);
};

proto._findByIndex = function(indexKey, indexValue, fields, opts){
    var indexCollection = this.conn.getCollection(this._indexCollectionName(indexKey), true);

    var self = this;
    return P.try(function(){
        return indexCollection.findById(indexValue, 'ids', opts);
    })
    .then(function(doc){
        var ids = doc ? Object.keys(doc.ids) : [];
        if(opts && opts.limit){
            ids = ids.slice(0, opts.limit);
        }
        return P.mapSeries(ids, function(id){
            id = utils.unescapeField(id);
            return self.findById(id, fields, opts)
            .then(function(doc){
                if(!doc){
                    throw new Error('index - ' + indexKey + ' is corrupted, please rebuild index');
                }
                return doc;
            });
        });
    });
};

proto.update = function(query, modifier, opts){
    opts = opts || {};
    var self = this;

    return P.try(function(){
        return self.find(query, '_id');
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
            .thenReturn(1);
        }

        if(!Array.isArray(ret)){
            return self._updateById(ret._id, modifier, opts)
            .thenReturn(1);
        }
        return P.each(ret, function(doc){
            return self._updateById(doc._id, modifier, opts);
        })
        .thenReturn(ret.length);
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
        return self.find(query, '_id');
    })
    .then(function(ret){
        if(!ret || ret.length === 0){
            return 0;
        }
        if(!Array.isArray(ret)){
            return self._removeById(ret._id, opts)
            .thenReturn(1);
        }
        return P.each(ret, function(doc){
            return self._removeById(doc._id, opts);
        })
        .thenReturn(ret.length);
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

// indexKey: json encoded sorted fields array
// indexValue: json encoded sorted fields value
proto._insertIndex = function(id, indexKey, indexValue, config){
    // Escape id since field name can not contain '$' or '.'
    id = utils.escapeField(id);

    var indexCollection = this.conn.getCollection(this._indexCollectionName(indexKey), true);

    if(config.unique){
        var doc = {_id : indexValue, 'ids' : {}, 'count' : 1};
        doc.ids[id] = 1;

        return indexCollection.insert(doc)
        .catch(function(err){
            if(err.message === 'doc already exists'){
                throw new Error('duplicate value for unique key - ' + indexKey);
            }
            throw err;
        });
    }

    var param = {
        $set : {},
        $inc : {count : 1},
    };
    param.$set['ids.' + id] = 1;

    var self = this;
    return P.try(function(){
        return indexCollection.find(indexValue, 'count');
    })
    .then(function(ret){
        if(ret && ret.count >= (config.maxCollision || DEFAULT_MAX_COLLISION)){
            throw new Error('too many documents have value - ' + indexValue + ' for index - ' + indexKey);
        }
        return indexCollection.update(indexValue, param, {upsert : true});
    });
};

// value is json encoded
proto._removeIndex = function(id, indexKey, indexValue, config){
    id = utils.escapeField(id);

    var indexCollection = this.conn.getCollection(this._indexCollectionName(indexKey), true);

    return P.try(function(){
        var param = {
            $unset : {},
            $inc: {count : -1}
        };
        param.$unset['ids.' + id] = 1;
        return indexCollection.update(indexValue, param);
    })
    .then(function(){
        return indexCollection.find(indexValue, 'count');
    })
    .then(function(ret){
        if(!ret){ // This should not happen
            throw new Error('index - ' + indexKey + ' corrputed, please rebuild index');
        }
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
    .finally(function(){
        delete self.pendingIndexTasks[id];
        // Restore domain
        process.domain = d;
    });
};

// 'index.name.key1.key2'
proto._indexCollectionName = function(indexKey){
    var keys = JSON.parse(indexKey).map(function(key){
        return utils.escapeField(key);
    });
    return 'index.' + utils.escapeField(this.name) + '.' + keys.join('.');
};

proto._key = function(id){
    return this.name + '$' + id;
};

proto._checkId = function(id){
    if(typeof(id) === 'string'){
        return id;
    }
    else if(typeof(id) === 'number'){
        return id.toString();
    }
    throw new Error('id must be number or string');
};

//http://docs.mongodb.org/manual/reference/limits/#Restriction-on-Collection-Names
proto._checkName = function(name){
    if(!name){
        throw new Error('Collection name can not empty');
    }
    if(typeof(name) !== 'string'){
        throw new Error('Collection name must be string');
    }
    if(name.indexOf('$') !== -1){
        throw new Error('Collection name can not contain "$"');
    }
    if(name.indexOf('system.') === 0){
        throw new Error('Collection name can not begin with "system."');
    }
};

module.exports = Collection;
