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

var utils = require('./utils');

//http://docs.mongodb.org/manual/reference/limits/#Restrictions-on-Field-Names
var verifyDoc = function(doc){
    if(doc === null || typeof(doc) !== 'object'){
        return;
    }

    for(var key in doc){
        if(key[0] === '$'){
            throw new Error('Document fields can not start with "$"');
        }
        if(key.indexOf('.') !== -1){
            throw new Error('Document fields can not contain "."');
        }
        verifyDoc(doc[key]);
    }
};

exports.$insert = function(doc, param){
    param = param || {};
    if(doc !== null){
        throw new Error('doc already exists');
    }
    verifyDoc(param);
    return param;
};

exports.$replace = function(doc, param){
    param = param || {};
    if(doc === null){
        throw new Error('doc not exist');
    }
    verifyDoc(param);
    return param;
};

exports.$remove = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    return null;
};

exports.$set = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        verifyDoc(param[path]);
        utils.setObjPath(doc, path, param[path]);
    }
    return doc;
};

exports.$unset = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        if(!!param[path]){
            utils.deleteObjPath(doc, path);
        }
    }
    return doc;
};

exports.$inc = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        var value = utils.getObjPath(doc, path);
        var delta = param[path];
        if(value === undefined){
            value = 0;
        }
        if(typeof(value) !== 'number' || typeof(delta) !== 'number'){
            throw new Error('$inc non-number');
        }
        utils.setObjPath(doc, path, value + delta);
    }
    return doc;
};

exports.$push = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        var arr = utils.getObjPath(doc, path);
        if(arr === undefined){
            utils.setObjPath(doc, path, []);
            arr = utils.getObjPath(doc, path);
        }
        if(!Array.isArray(arr)){
            throw new Error('$push to non-array');
        }
        verifyDoc(param[path]);
        arr.push(param[path]);
    }
    return doc;
};

exports.$pushAll = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        var arr = utils.getObjPath(doc, path);
        if(arr === undefined){
            utils.setObjPath(doc, path, []);
            arr = utils.getObjPath(doc, path);
        }
        if(!Array.isArray(arr)){
            throw new Error('$push to non-array');
        }
        var items = param[path];
        if(!Array.isArray(items)){
            items = [items];
        }
        for(var i in items){
            verifyDoc(items[i]);
            arr.push(items[i]);
        }
    }
    return doc;
};

exports.$addToSet = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        var arr = utils.getObjPath(doc, path);
        if(arr === undefined){
            utils.setObjPath(doc, path, []);
            arr = utils.getObjPath(doc, path);
        }
        if(!Array.isArray(arr)){
            throw new Error('$addToSet to non-array');
        }
        var value = param[path];
        if(arr.indexOf(value) === -1){
            verifyDoc(value);
            arr.push(value);
        }
    }
    return doc;
};

exports.$pop = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        var arr = utils.getObjPath(doc, path);
        if(Array.isArray(arr)){
            arr.pop();
        }
    }
    return doc;
};

exports.$pull = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        var arr = utils.getObjPath(doc, path);
        if(Array.isArray(arr)){
            var value = param[path];
            var i = arr.indexOf(value);
            if(i !== -1){
                arr.splice(i, 1);
            }
        }
    }
    return doc;
};

exports.$pullAll = function(doc, param){
    if(doc === null){
        throw new Error('doc not exist');
    }
    for(var path in param){
        var arr = utils.getObjPath(doc, path);
        if(Array.isArray(arr)){
            var values = param[path];
            if(!Array.isArray(values)){
                values = [values];
            }
            values.forEach(function(value){
                var i = arr.indexOf(value);
                if(i !== -1){
                    arr.splice(i, 1);
                }
            }); //jshint ignore:line
        }
    }
    return doc;
};
