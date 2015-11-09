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

var _ = require('lodash');
var util = require('util');
var P = require('bluebird');
var child_process = require('child_process');
var uuid = require('node-uuid');

// Add some usefull promise methods
exports.extendPromise = function(P){
    // This is designed for large array
    // The original map with concurrency option does not release memory
    P.mapLimit = function(items, fn, limit){
        if(!limit){
            limit = 1000;
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
        return promise.thenReturn(results);
    };

    P.mapSeries = function(items, fn){
        var results = [];
        return P.each(items, function(item){
            return P.try(function(){
                return fn(item);
            })
            .then(function(ret){
                results.push(ret);
            });
        })
        .thenReturn(results);
    };
};

exports.uuid = function(){
    return uuid.v4();
};

exports.isEmpty = function(obj){
    for(var key in obj){
        return false;
    }
    return true;
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
            throw new Error('field ' + path + ' exists and not a object');
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

exports.isDict = function(obj){
    return typeof(obj) === 'object' && obj !== null && !Array.isArray(obj);
};

// escape '$' and '.' in field name
// '$abc.def\\g' => '\\u0024abc\\u002edef\\\\g'
exports.escapeField = function(str){
    return str.replace(/\\/g, '\\\\').replace(/\$/g, '\\u0024').replace(/\./g, '\\u002e');
};

exports.unescapeField = function(str){
    return str.replace(/\\u002e/g, '.').replace(/\\u0024/g, '$').replace(/\\\\/g, '\\');
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

exports.remoteExec = function(ip, cmd, opts){
    ip = ip || '127.0.0.1';
    opts = opts || {};
    var user = opts.user || process.env.USER;
    var successCodes = opts.successCodes || [0];

    var child = null;
    // localhost with current user
    if((ip === '127.0.0.1' || ip.toLowerCase() === 'localhost') && user === process.env.USER){
        child = child_process.spawn('bash', ['-c', cmd]);
    }
    // run remote via ssh
    else{
        child = child_process.spawn('ssh', ['-o StrictHostKeyChecking=no', user + '@' + ip, 'bash -c \'' + cmd + '\'']);
    }

    var deferred = P.defer();
    var stdout = '', stderr = '';
    child.stdout.on('data', function(data){
        stdout += data;
    });
    child.stderr.on('data', function(data){
        stderr += data;
    });
    child.on('exit', function(code, signal){
        if(successCodes.indexOf(code) !== -1){
            deferred.resolve(stdout);
        }
        else{
            deferred.reject(new Error(util.format('remoteExec return code %s on %s@%s - %s\n%s', code, user, ip, cmd, stderr)));
        }
    });
    return deferred.promise;
};

exports.waitUntil = function(fn, checkInterval){
    if(!checkInterval){
        checkInterval = 100;
    }

    var deferred = P.defer();
    var check = function(){
        if(fn()){
            deferred.resolve();
        }
        else{
            setTimeout(check, checkInterval);
        }
    };
    check();

    return deferred.promise;
};

exports.rateCounter = function(opts){
    opts = opts || {};
    var perserveSeconds = opts.perserveSeconds || 3600;
    var sampleSeconds = opts.sampleSeconds || 5;

    var counts = {};
    var cleanInterval = null;

    var getCurrentSlot = function(){
        return Math.floor(Date.now() / 1000 / sampleSeconds);
    };

    var beginSlot = getCurrentSlot();

    var counter = {
        inc : function(){
            var slotNow = getCurrentSlot();
            if(!counts.hasOwnProperty(slotNow)){
                counts[slotNow] = 0;
            }
            counts[slotNow]++;
        },

        reset : function(){
            counts = {};
            beginSlot = getCurrentSlot();
        },

        clean : function(){
            var slotNow = getCurrentSlot();
            Object.keys(counts).forEach(function(slot){
                if(slot < slotNow - Math.floor(perserveSeconds / sampleSeconds)){
                    delete counts[slot];
                }
            });
        },

        rate : function(lastSeconds){
            var slotNow = getCurrentSlot();
            var total = 0;
            var startSlot = slotNow - Math.floor(lastSeconds / sampleSeconds);
            if(startSlot < beginSlot){
                startSlot = beginSlot;
            }
            for(var slot = startSlot; slot < slotNow; slot++){
                total += counts[slot] || 0;
            }
            return total / ((slotNow - startSlot) * sampleSeconds);
        },

        stop : function(){
            clearInterval(cleanInterval);
        },

        counts : function(){
            return counts;
        }
    };

    cleanInterval = setInterval(function(){
        counter.clean();
    }, sampleSeconds * 1000);

    return counter;
};

exports.hrtimer = function(autoStart){
    var total = 0;
    var starttime = null;

    var timer = {
        start : function(){
            if(starttime){
                return;
            }
            starttime = process.hrtime();
        },
        stop : function(){
            if(!starttime){
                return;
            }
            var timedelta = process.hrtime(starttime);
            total += timedelta[0] * 1000 + timedelta[1] / 1000000;
            return total;
        },
        total : function(){
            return total; //in ms
        },
    };

    if(autoStart){
        timer.start();
    }
    return timer;
};

exports.timeCounter = function(){
    var counts = {};

    return {
        add : function(name, time){
            if(!counts.hasOwnProperty(name)){
                counts[name] = [0, 0, 0]; // total, count, average
            }
            var count = counts[name];
            count[0] += time;
            count[1]++;
            count[2] = count[0] / count[1];
        },
        reset : function(){
            counts = {};
        },
        getCounts : function(){
            return counts;
        },
    };
};


// trick v8 to not use hidden class
// https://github.com/joyent/node/issues/25661
exports.forceHashMap = function(){
    var obj = {k : 1};
    delete obj.k;
    return obj;
};
