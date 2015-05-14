'use strict';

var utils = require('./utils');

module.exports = {
    $insert : function(doc, param){
        param = param || {};
        if(doc !== null){
            throw new Error('doc already exists');
        }
        return utils.clone(param);
    },
    $replace : function(doc, param){
        param = param || {};
        if(doc === null){
            throw new Error('doc not exist');
        }
        return utils.clone(param);
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
            utils.setObjPath(doc, path, utils.clone(param[path]));
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
            arr.push(utils.clone(param[path]));
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
                arr.push(utils.clone(items[i]));
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
                arr.push(utils.clone(value));
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
