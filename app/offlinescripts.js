'use strict';

/**
 * Offline scripts
 *
 * WARNING: Shutdown all cluster before running these scripts
 */

var P = require('bluebird');
var backends = require('./backends');
var Document = require('./document'); //jshint ignore:line
var Collection = require('./collection');
var utils = require('./utils');
var logger = require('memdb-logger').getLogger('memdb', __filename);

// dropIndex('field1 field2')
exports.dropIndex = function(backendConf, collName, keys){
    if(!Array.isArray(keys)){
        keys = keys.split(' ');
    }
    var indexKey = JSON.stringify(keys.sort());
    var backend = backends.create(backendConf);

    return P.try(function(){
        return backend.start();
    })
    .then(function(){
        var indexCollName = Collection.prototype._indexCollectionName.call({name : collName}, indexKey);
        return backend.drop(indexCollName);
    })
    .finally(function(){
        logger.warn('Droped index %s %s', collName, indexKey);
        return backend.stop();
    });
};

// rebuildIndex('field1 field2', {unique : true})
exports.rebuildIndex = function(backendConf, collName, keys, opts){
    opts = opts || {};
    if(!Array.isArray(keys)){
        keys = keys.split(' ');
    }
    var indexKey = JSON.stringify(keys.sort());
    var backend = backends.create(backendConf);

    logger.warn('Start rebuild index %s %s', collName, indexKey);
    return P.try(function(){
        return exports.dropIndex(backendConf, collName, keys);
    })
    .then(function(){
        return backend.start();
    })
    .then(function(){
        return backend.getAll(collName);
    })
    .then(function(itor){
        return utils.mongoForEach(itor, function(item){
            var indexValue = Document.prototype._getIndexValue.call({changed : item}, indexKey, opts);
            if(!indexValue){
                return;
            }
            var indexCollName = Collection.prototype._indexCollectionName.call({name : collName}, indexKey);

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

                var id64 = new Buffer(item._id).toString('base64');
                doc.ids[id64] = 1;
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
