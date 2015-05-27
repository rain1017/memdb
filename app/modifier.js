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
    if(!utils.isDict(doc)){
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

module.exports = {
    $insert : function(doc, param){
        param = param || {};
        if(doc !== null){
            throw new Error('doc already exists');
        }
        verifyDoc(param);
        return clone(param);
    },
    $replace : function(doc, param){
        param = param || {};
        if(doc === null){
            throw new Error('doc not exist');
        }
        verifyDoc(param);
        return clone(param);
    },
    $remove : function(doc, param){
        if(doc === null){
            throw new Error('doc not exist');
        }
        return null;
    },
    $set : function(doc, param){
        if(doc === null){
            throw new Error('doc not exist');
        }
        for(var path in param){
            verifyDoc(param[path]);
            utils.setObjPath(doc, path, clone(param[path]));
        }
        return doc;
    },
    $unset : function(doc, param){
        if(doc === null){
            throw new Error('doc not exist');
        }
        for(var path in param){
            if(!!param[path]){
                utils.deleteObjPath(doc, path);
            }
        }
        return doc;
    },
    $inc : function(doc, param){
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
                throw new Error('not a number');
            }
            utils.setObjPath(doc, path, value + delta);
        }
        return doc;
    },
    $push : function(doc, param){
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
                throw new Error('not an array');
            }
            verifyDoc(param[path]);
            arr.push(clone(param[path]));
        }
        return doc;
    },
    $pushAll : function(doc, param){
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
                throw new Error('not an array');
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
    },
    $addToSet : function(doc, param){
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
                throw new Error('not an array');
            }
            var value = param[path];
            var exist = false;
            for(var i in arr){
                if(arr[i] === value){
                    exist = true;
                    break;
                }
            }
            if(!exist){
                verifyDoc(value);
                arr.push(clone(value));
            }
        }
        return doc;
    },
    $pop : function(doc, param){
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
    },
    $pull : function(doc, param){
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
    },
};
