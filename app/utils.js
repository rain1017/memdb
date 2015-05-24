'use strict';

var _ = require('lodash');
var P = require('bluebird');

var exports = {};

// Make sure err is instanceof Error
exports.normalizecb = function(cb){
    return function(err, ret){
        if(!!err && !(err instanceof Error)){
            err = new Error(err);
        }
        cb(err, ret);
    };
};

// Add mapLimit to promise
// not using concurrency = xxx option since it doesn't release memory
exports.promiseSetLimit = function(P, defaultLimit){
    P.mapLimit = function(items, fn, limit){
        if(!limit){
            limit = defaultLimit;
        }
        var groups = [];
        var group = [];
        items.forEach(function(item){
            group.push(item);
            if(group.length >= limit){
                groups.push(group);
                group = [];
            }
        });
        if(group.length > 0){
            groups.push(group);
        }

        var results = [];
        var promise = P.resolve();
        groups.forEach(function(group){
            promise = promise.then(function(){
                return P.map(group, fn)
                .then(function(ret){
                    ret.forEach(function(item){
                        results.push(item);
                    });
                });
            });
        });
        return promise.then(function(){
            return results;
        });
    };
};




exports.injectLogger = function(pomeloLogger){
    if(pomeloLogger.__memdb__){
        return; // already injected
    }
    pomeloLogger.__memdb__ = true;

    var getLogger = pomeloLogger.getLogger;

    pomeloLogger.getLogger = function(){
        var logger = getLogger.apply(this, arguments);
        var methods = ['log', 'debug', 'info', 'warn', 'error', 'trace', 'fatal'];

        methods.forEach(function(method){
            var originMethod = logger[method];
            logger[method] = function(){
                var prefix = '';
                if(process.domain && process.domain.__memdb__){
                    prefix += '[trans:' + process.domain.__memdb__.trans + '] ';
                }
                if(arguments.length > 0){
                    arguments[0] = prefix + arguments[0];
                }
                return originMethod.apply(this, arguments);
            };
        });

        return logger;
    };
};

exports.getObjPath = function(obj, path){
    var current = obj;
    path.split('.').forEach(function(field){
        if(!!current){
            current = current[field];
        }
    });
    return current;
};

exports.setObjPath = function(obj, path, value){
    if(typeof(obj) !== 'object'){
        throw new Error('not object');
    }
    var current = obj;
    var fields = path.split('.');
    var finalField = fields.pop();
    fields.forEach(function(field){
        if(!current.hasOwnProperty(field)){
            current[field] = {};
        }
        current = current[field];
        if(typeof(current) !== 'object'){
            throw new Error('path is exist and not a object');
        }
    });
    current[finalField] = value;
};

exports.deleteObjPath = function(obj, path){
    if(typeof(obj) !== 'object'){
        throw new Error('not object');
    }
    var current = obj;
    var fields = path.split('.');
    var finalField = fields.pop();
    fields.forEach(function(field){
        if(!!current){
            current = current[field];
        }
    });
    if(current !== undefined){
        delete current[finalField];
    }
};

exports.clone = function(obj){
    return JSON.parse(JSON.stringify(obj));
};

exports.cloneEx = function(obj){
    if(obj === null || obj === undefined || typeof(obj) === 'number' || typeof(obj) === 'string' || typeof(obj) === 'boolean'){
        return obj;
    }
    if(typeof(obj) === 'object'){
        var copy = Array.isArray(obj) ? new Array(obj.length) : {};
        for(var key in obj){
            copy[key] = exports.clone(obj[key]);
        }
        return copy;
    }
    throw new Error('unsupported type of obj: ' + obj);
};

exports.uuid = function(){
    // return a short uuid, based on current tick and a random number
    // result uuid like '2mnh1r3wb'
    return ((Date.now() - 1422720000000) * 1000 + _.random(1000)).toString(36);
};

exports.isDict = function(obj){
    return typeof(obj) === 'object' && obj !== null && !Array.isArray(obj);
};

// Async foreach for mongo's cursor
exports.mongoForEach = function(itor, func){
    var deferred = P.defer();

    var next = function(err){
        if(err){
            return deferred.reject(err);
        }

        // async iterator with .next(cb)
        itor.next(function(err, value){
            if(err){
                return deferred.reject(err);
            }
            if(value === null){
                return deferred.resolve();
            }
            P.try(function(){
                return func(value);
            })
            .nodeify(next);
        });
    };
    next();

    return deferred.promise;
};

module.exports = exports;
