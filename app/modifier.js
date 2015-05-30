'use strict';

var utils = require('./utils');

// Max document size 1MB after serialize
var MAX_DOC_SIZE = 1024 * 1024;

var clone = function(obj){
    var json = JSON.stringify(obj);
    if(json.length > MAX_DOC_SIZE){
        throw new Error('Document size exceed limit');
    }
    return JSON.parse(json);
};

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
    return clone(param);
};

exports.$replace = function(doc, param){
    param = param || {};
    if(doc === null){
        throw new Error('doc not exist');
    }
    verifyDoc(param);
    return clone(param);
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
        utils.setObjPath(doc, path, clone(param[path]));
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
        arr.push(clone(param[path]));
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
            arr.push(clone(items[i]));
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
            arr.push(clone(value));
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
            for(var i in arr){
                if(arr[i] === value){
                    arr.splice(i, 1);
                    break;
                }
            }
        }
    }
    return doc;
};
