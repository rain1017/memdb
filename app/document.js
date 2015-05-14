'use strict';

var P = require('bluebird');
var util = require('util');
var utils = require('./utils');
var AsyncLock = require('async-lock');
var EventEmitter = require('events').EventEmitter;
var modifier = require('./modifier');
var logger = require('pomelo-logger').getLogger('memdb', __filename);

var DEFAULT_LOCK_TIMEOUT = 10 * 1000;

/**
 *
 * Events:
 *
 * updateIndex - (connectionId, indexKey, oldValue, newValue)
 * Used for internal index update (won't fire on commit or rollback)
 */
var Document = function(opts){ //jshint ignore:line
    opts = opts || {};

    var doc = opts.doc || null;
    if(typeof(doc) !== 'object'){
        throw new Error('doc must be object');
    }

    this._id = opts._id;
    this.commited = doc;
    this.changed = undefined; // undefined means no change (while null means removed)

    this.indexes = opts.indexes || {};

    this.connectionId = null; // Connection that hold the document lock
    this._lock = new AsyncLock({Promise : P, timeout : opts.lockTimeout || DEFAULT_LOCK_TIMEOUT});
    this.releaseCallback = null;

    EventEmitter.call(this);
};

util.inherits(Document, EventEmitter);

var proto = Document.prototype;

/**
 * connectionId - current connection
 */
proto.find = function(connectionId, fields){
    var doc = this.isLocked(connectionId) ? this.changed : this.commited;

    if(doc === null){
        return null;
    }

    if(!fields){
        return utils.clone(doc);
    }

    var includeFields = [], excludeFields = [];

    if(typeof(fields) === 'string'){
        includeFields = fields.split(' ');
    }
    else if(typeof(fields) === 'object'){
        for(var field in fields){
            if(!!fields[field]){
                includeFields.push(field);
            }
            else{
                excludeFields.push(field);
            }
        }
        if(includeFields.length > 0 && excludeFields.length > 0){
            throw new Error('Can not specify both include and exclude fields');
        }
    }

    var ret = {};
    if(includeFields.length > 0){
        includeFields.forEach(function(field){
            if(doc.hasOwnProperty(field)){
                ret[field] = utils.clone(doc[field]);
            }
        });
    }
    else{
        ret = utils.clone(doc);
        excludeFields.forEach(function(field){
            delete ret[field];
        });
    }

    return ret;
};

proto.exists = function(connectionId){
    return this.isLocked(connectionId) ? this.changed !== null: this.commited !== null;
};

proto.insert = function(connectionId, doc){
    this.modify(connectionId, '$insert',  doc);
};

proto.remove = function(connectionId){
    this.modify(connectionId, '$remove');
};

proto.update = function(connectionId, doc, opts){
    opts = opts || {};
    doc = doc || {};

    if(this.changed === null && opts.upsert){
        this.modify(connectionId, '$insert', {});
    }

    var isModify = false;
    for(var field in doc){
        if(field.slice(0, 1) === '$'){
            isModify = true;
            break;
        }
    }

    if(!isModify){
        this.modify(connectionId, '$replace', doc);
    }
    else{
        for(var cmd in doc){
            this.modify(connectionId, cmd, doc[cmd]);
        }
    }
};

proto.modify = function(connectionId, cmd, param){
    this.ensureLocked(connectionId);

    var oldValues = {};
    for(var indexKey in this.indexes){
        oldValues[indexKey] = this._getIndexValue(indexKey, this.indexes[indexKey]);
    }

    var modifyFunc = modifier[cmd];
    if(typeof(modifyFunc) !== 'function'){
        throw new Error('invalid modifier - ' + cmd);
    }

    this.changed = modifyFunc(this.changed, param);

    // id is immutable
    if(!!this.changed){
        this.changed._id = this._id;
    }

    for(indexKey in this.indexes){
        var value = this._getIndexValue(indexKey, this.indexes[indexKey]);
        if(oldValues[indexKey] !== value){
            logger.trace('updateIndex %s %s %s', indexKey, oldValues[indexKey], value);

            this.emit('updateIndex', connectionId, indexKey, oldValues[indexKey], value);
        }
    }

    logger.trace('%s %s %j => %j', this._id, cmd, param, this.changed);
};

proto.lock = function(connectionId){
    var self = this;
    if(connectionId === null || connectionId === undefined){
        throw new Error('connectionId is null');
    }

    var deferred = P.defer();
    if(self.isLocked(connectionId)){
        deferred.resolve();
    }
    else{
        self._lock.acquire('', function(release){
            self.connectionId = connectionId;
            self.releaseCallback = release;
            self.changed = utils.clone(self.commited);
            deferred.resolve();
        })
        .catch(function(err){
            deferred.reject(err);
        });
    }
    return deferred.promise;
};

proto._unlock = function(){
    if(this.connectionId === null){
        return;
    }
    this.connectionId = null;
    var releaseCallback = this.releaseCallback;
    this.releaseCallback = null;
    process.nextTick(releaseCallback);
};

proto._getChanged = function(){
    return this.changed !== undefined ? this.changed : this.commited;
};

proto.commit = function(connectionId){
    this.ensureLocked(connectionId);

    this.commited = this.changed;
    this.changed = undefined;

    this._unlock();
    this.emit('commit');
};

proto.rollback = function(connectionId){
    this.ensureLocked(connectionId);

    this.changed = undefined;

    this._unlock();
    this.emit('rollback');
};

proto.ensureLocked = function(connectionId){
    if(!this.isLocked(connectionId)){
        throw new Error('doc not locked by ' + connectionId);
    }
};

proto.isLocked = function(connectionId){
    return this.connectionId === connectionId && connectionId !== null && connectionId !== undefined;
};

proto._getIndexValue = function(indexKey, opts){
    opts = opts || {};

    var self = this;
    var indexValue = JSON.parse(indexKey).map(function(key){
        var value = !!self.changed ? self.changed[key] : undefined;
        if(value === null || value === undefined){
            return undefined;
        }
        if(['number', 'string', 'boolean'].indexOf(typeof(value)) === -1){
            throw new Error('invalid value for indexed key ' + indexKey);
        }
        var ignores = opts.valueIgnore ? opts.valueIgnore[key] || [] : [];
        if(ignores.indexOf(value) !== -1){
            return undefined;
        }
        return value;
    });

    // Return null if one of the value is undefined
    if(indexValue.indexOf(undefined) !== -1){
        return null;
    }
    return JSON.stringify(indexValue);
};

module.exports = Document;
