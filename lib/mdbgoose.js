'use strict';

var path = require('path');
global.MONGOOSE_DRIVER_PATH = '../../../../lib/mongoose-driver';
var P = require('bluebird');
var util = require('util');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);
var memdb = require('./index');

var mongoose = require('mongoose');

// Rename original connect
mongoose.connectMongo = mongoose.connect;
mongoose.disconnectMongo = mongoose.disconnect;

mongoose.connect = function(opts){
    var promise = P.bind(this)
    .then(function(){
        if(this._autoconn){
            throw new Error('Already connected');
        }
        return memdb.autoConnect(opts);
    })
    .then(function(ret){
        this._autoconn = ret;
    });

    var cb = arguments[arguments.length - 1];
    if(typeof(cb) === 'function'){
        return promise.nodeify(cb);
    }
    else{
        return promise;
    }
};

mongoose.disconnect = function(){
    var promise = P.bind(this)
    .then(function(){
        return this.autoconn.close();
    })
    .then(function(){
        this._autoconn = null;
    });

    var cb = arguments[arguments.length - 1];
    if(typeof(cb) === 'function'){
        return promise.nodeify(cb);
    }
    else{
        return promise;
    }
};

Object.defineProperty(mongoose, 'autoconn' , {
    get : function(){
        if(!mongoose._autoconn){
            throw new Error('Please connect first');
        }
        return mongoose._autoconn;
    }
});

mongoose.transaction = function(func, shardId, cb){
    var promise = this.autoconn.transaction(func, shardId);

    if(typeof(cb) === 'function'){
        return promise.nodeify(cb);
    }
    else{
        return promise;
    }
};

// Parse mongoose model to generate collection config (indexes)
mongoose.genCollectionConfig = function(){
    var collections = {};

    for(var name in mongoose.models){
        var model = mongoose.models[name];
        var schema = model.schema;
        var collname = model.collection.name;

        if(!collections[collname]){
            collections[collname] = {};
        }
        var collection = collections[collname];
        if(!collection.indexes){
            collection.indexes = [];
        }

        var paths = schema.paths;
        var index = null, mdbIndex = null;
        for(var field in paths){
            if(field === '_id' || field.indexOf('.') !== -1){
                continue; //ignore compound field and _id
            }
            index = paths[field]._index;
            if(!!index){
                mdbIndex = {keys : [field]};
                if(!!index.unique){
                    mdbIndex.unique = true;
                }

                mdbIndex.valueIgnore = {};
                mdbIndex.valueIgnore[field] = paths[field].options.indexIgnore || [];
                collection.indexes.push(mdbIndex);
            }
        }

        if(!!schema._indexes){
            for(var i in schema._indexes){
                index = schema._indexes[i];
                mdbIndex = {keys : [], valueIgnore : {}};
                for(field in index[0]){
                    if(index[0][field]){
                        mdbIndex.keys.push(field);
                        mdbIndex.valueIgnore[field] = paths[field].options.indexIgnore || [];
                    }
                }
                if(index[1] && !!index[1].unique){
                    mdbIndex.unique = true;
                }
                collection.indexes.push(mdbIndex);
            }
        }

        //Disable versionkey
        schema.options.versionKey = false;
    }

    logger.info('parsed collection config : %j', collections);
    return collections;
};


var Model = mongoose.Model;

Model.findMongo = function(query, cb){
    return this.find.apply(this, [].slice.call(arguments)).comment('$mongo');
};

Model.findOneMongo = function(){
    return this.findOne.apply(this, [].slice.call(arguments)).comment('$mongo');
};

Model.findByIdMongo = function(){
    return this.findById.apply(this, [].slice.call(arguments)).comment('$mongo');
};

Model.findReadOnly = function(){
    return this.find.apply(this, [].slice.call(arguments)).comment('$readonly');
};

Model.findOneReadOnly = function(){
    return this.findOne.apply(this, [].slice.call(arguments)).comment('$readonly');
};

Model.findByIdReadOnly = function(){
    return this.findById.apply(this, [].slice.call(arguments)).comment('$readonly');
};

// var overwrited = ['insert', 'save', 'remove', 'update'];

// for (var i = overwrited.length - 1; i >= 0; i--) {
//     (function(funcName){
//         var oldFunc = Model.prototype[funcName];
//         Model.prototype[funcName] = function (fn) {
//             if(this.__readonly__) {
//                 return fn(new Error('document is readonly'));
//             }
//             oldFunc.apply(this, arguments);
//         };
//     })(overwrited[i]); //jshint ignore:line
// }

var overwrited = ['findByIdAndRemove', 'findByIdAndUpdate', 'findOneAndUpdate', 'findOneAndRemove'];

for (var i = overwrited.length - 1; i >= 0; i--) {
    (function(funcName){
        mongoose.Model[funcName] = function(){
            var cb = arguments[arguments.length-1];
            if(typeof(cb) === 'function') {
                cb(new Error(util.format('not implemented method: ', funcName)));
            } else {
                throw new Error(util.format('not implemented method: ', funcName));
            }
        };
    })(overwrited[i]); //jshint ignore:line
}

module.exports = P.promisifyAll(mongoose);

