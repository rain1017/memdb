// Copyright 2015 rain1017.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

'use strict';

var P = require('bluebird');
var util = require('util');
var utils = require('./utils');
var AsyncLock = require('async-lock');
var EventEmitter = require('events').EventEmitter;
var modifier = require('./modifier');
var logger = require('memdb-logger').getLogger('memdb', __filename);

var DEFAULT_LOCK_TIMEOUT = 10 * 1000;

var Document = function(opts){ //jshint ignore:line
    opts = opts || {};

    if(!opts.hasOwnProperty('_id')){
        throw new Error('_id is not specified');
    }
    this._id = opts._id;

    var doc = opts.doc || null;
    if(typeof(doc) !== 'object'){
        throw new Error('doc must be object');
    }
    if(!!doc){
        doc._id = this._id;
    }

    this.commited = doc;
    this.changed = undefined; // undefined means no change, while null means removed
    this.connId = null; // Connection that hold the document lock

    this.locker = opts.locker;
    this.lockKey = opts.lockKey;
    if(!this.locker){
        this.locker = new AsyncLock({
                            Promise : P,
                            timeout : opts.lockTimeout || DEFAULT_LOCK_TIMEOUT
                            });
        this.lockKey = '';
    }

    this.releaseCallback = null;

    this.indexes = opts.indexes || {};

    this.savedIndexValues = {}; //{indexKey : indexValue}

    EventEmitter.call(this);
};

util.inherits(Document, EventEmitter);

var proto = Document.prototype;

proto.find = function(connId, fields){
    var doc = this.isLocked(connId) ? this._getChanged() : this.commited;

    if(doc === null){
        return null;
    }

    if(!fields){
        return doc;
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

    var ret = null;
    if(includeFields.length > 0){
        ret = {};
        includeFields.forEach(function(field){
            if(doc.hasOwnProperty(field)){
                ret[field] = doc[field];
            }
        });
        ret._id = this._id;
    }
    else if(excludeFields.length > 0){
        ret = {};
        for(var key in doc){
            ret[key] = doc[key];
        }
        excludeFields.forEach(function(key){
            delete ret[key];
        });
    }
    else{
        ret = doc;
    }

    return ret;
};

proto.exists = function(connId){
    return this.isLocked(connId) ? this._getChanged() !== null: this.commited !== null;
};

proto.insert = function(connId, doc){
    this.modify(connId, '$insert',  doc);
};

proto.remove = function(connId){
    this.modify(connId, '$remove');
};

proto.update = function(connId, modifier, opts){
    opts = opts || {};
    if(!modifier){
        throw new Error('modifier is empty');
    }

    modifier = modifier || {};

    var isModify = false;
    for(var field in modifier){
        isModify = (field[0] === '$');
        break;
    }

    if(!isModify){
        this.modify(connId, '$replace', modifier);
    }
    else{
        for(var cmd in modifier){
            this.modify(connId, cmd, modifier[cmd]);
        }
    }
};

proto.modify = function(connId, cmd, param){
    this.ensureLocked(connId);

    for(var indexKey in this.indexes){
        if(!this.savedIndexValues.hasOwnProperty(indexKey)){
            this.savedIndexValues[indexKey] = this._getIndexValue(indexKey, this.indexes[indexKey]);
        }
    }

    var modifyFunc = modifier[cmd];
    if(typeof(modifyFunc) !== 'function'){
        throw new Error('invalid modifier - ' + cmd);
    }

    if(this.changed === undefined){ //copy on write
        this.changed = utils.clone(this.commited);
    }

    this.changed = modifyFunc(this.changed, param);

    // id is immutable
    if(!!this.changed){
        this.changed._id = this._id;
    }

    for(indexKey in this.indexes){
        var value = this._getIndexValue(indexKey, this.indexes[indexKey]);

        if(value !== this.savedIndexValues[indexKey]){
            logger.trace('%s.updateIndex(%s, %s, %s)', this._id, indexKey, this.savedIndexValues[indexKey], value);
            this.emit('updateIndex', connId, indexKey, this.savedIndexValues[indexKey], value);

            this.savedIndexValues[indexKey] = value;
        }
    }

    logger.trace('%s.modify(%s, %j) => %j', this._id, cmd, param, this.changed);
};

proto.lock = function(connId){
    if(connId === null || connId === undefined){
        throw new Error('connId is null');
    }

    var deferred = P.defer();
    if(this.isLocked(connId)){
        deferred.resolve();
    }
    else{
        var self = this;
        this.locker.acquire(this.lockKey, function(release){
            self.connId = connId;
            self.releaseCallback = release;

            self.emit('lock');
            deferred.resolve();
        })
        .catch(function(err){
            if(!deferred.isResolved()){
                deferred.reject(new Error('doc.lock failed - ' + self.lockKey));
            }
        });
    }
    return deferred.promise;
};

// Wait existing lock release (not create new lock)
proto._waitUnlock = function(){
    var deferred = P.defer();
    var self = this;
    this.locker.acquire(this.lockKey, function(){
        deferred.resolve();
    })
    .catch(function(err){
        deferred.reject(new Error('doc._waitUnlock failed - ' + self.lockKey));
    });
    return deferred.promise;
};

proto._unlock = function(){
    if(this.connId === null){
        return;
    }

    this.connId = null;
    var releaseCallback = this.releaseCallback;
    this.releaseCallback = null;

    releaseCallback();

    this.emit('unlock');
};

proto._getChanged = function(){
    return this.changed !== undefined ? this.changed : this.commited;
};

proto._getCommited = function(){
    return this.commited;
};

proto.commit = function(connId){
    this.ensureLocked(connId);

    if(this.changed !== undefined){
        this.commited = this.changed;
    }
    this.changed = undefined;

    this.emit('commit');
    this._unlock();
};

proto.rollback = function(connId){
    this.ensureLocked(connId);

    this.changed = undefined;

    this.savedIndexValues = {};

    this.emit('rollback');
    this._unlock();
};

proto.ensureLocked = function(connId){
    if(!this.isLocked(connId)){
        throw new Error('doc not locked by ' + connId);
    }
};

proto.isLocked = function(connId){
    return this.connId === connId && connId !== null && connId !== undefined;
};

proto.isFree = function(){
    return this.connId === null;
};

proto._getIndexValue = function(indexKey, opts){
    opts = opts || {};

    var self = this;
    var indexValue = JSON.parse(indexKey).sort().map(function(key){
        var doc = self._getChanged();
        var value = !!doc ? doc[key] : undefined;
        // null and undefined is not included in index
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
