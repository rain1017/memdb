'use strict';

/**
 * WARNING: Shutdown all cluster before running these scripts
 */

var P = require('bluebird');
var backends = require('./backends');
var BackendLocker = require('./backendLocker');
var Document = require('./document'); //jshint ignore:line
var Collection = require('./collection');
var utils = require('./utils');
var logger = require('memdb-logger').getLogger('memdb', __filename);

var ensureShutDown = function(lockingConf){
    lockingConf.shardId = '$';
    lockingConf.heartbeatInterval = 0;
    var backendLocker = new BackendLocker(lockingConf);

    return P.try(function(){
        return backendLocker.start();
    })
    .then(function(){
        return backendLocker.getActiveShards();
    })
    .then(function(shardIds){
        if(shardIds.length > 0){
            throw new Error('You should shutdown all shards first');
        }
    })
    .finally(function(){
        return backendLocker.stop();
    });
};

// dropIndex('field1 field2')
exports.drop = function(conf, collName, keys){
    if(!Array.isArray(keys)){
        keys = keys.split(' ');
    }
    var indexKey = JSON.stringify(keys.sort());
    conf.backend.shardId = '$';
    var backend = backends.create(conf.backend);
    var indexCollName = Collection.prototype._indexCollectionName.call({name : collName}, indexKey);

    return ensureShutDown(conf.locking)
    .then(function(){
        return backend.start();
    })
    .then(function(){
        return backend.drop(indexCollName);
    })
    .finally(function(){
        logger.warn('Droped index %s %s', collName, indexKey);
        return backend.stop();
    });
};

// rebuildIndex('field1 field2', {unique : true})
exports.rebuild = function(conf, collName, keys, opts){
    opts = opts || {};
    if(!Array.isArray(keys)){
        keys = keys.split(' ');
    }
    var indexKey = JSON.stringify(keys.sort());
    conf.backend.shardId = '$';
    var backend = backends.create(conf.backend);

    var indexCollName = Collection.prototype._indexCollectionName.call({name : collName}, indexKey);

    logger.warn('Start rebuild index %s %s', collName, indexKey);

    return ensureShutDown(conf.locking)
    .then(function(){
        return backend.start();
    })
    .then(function(){
        return backend.drop(indexCollName);
    })
    .then(function(){
        return backend.getAll(collName);
    })
    .then(function(itor){
        return utils.mongoForEach(itor, function(item){
            var indexValue = Document.prototype._getIndexValue.call({_getChanged : function(){return item;}}, indexKey, opts);
            if(!indexValue){
                return;
            }

            return P.try(function(){
                return backend.get(indexCollName, indexValue);
            })
            .then(function(doc){
                if(!doc){
                    doc = {_id: indexValue, ids : {}, count : 0};
                }
                else if(opts.unique){
                    throw new Error('Duplicate value for unique key ' + indexKey);
                }

                var id = utils.escapeField(item._id);
                doc.ids[id] = 1;
                doc.count++;
                return backend.set(indexCollName, indexValue, doc);
            });
        });
    })
    .then(function(){
        logger.warn('Finish rebuild index %s %s', collName, indexKey);
        return backend.stop();
    });
};
