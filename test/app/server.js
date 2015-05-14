'use strict';

var P = require('bluebird');
var _ = require('lodash');
var should = require('should');
var env = require('../env');
var logger = require('pomelo-logger').getLogger('test', __filename);

describe('server test', function(){
    beforeEach(env.flushdb);
    after(env.flushdb);

    it.only('start/stop server', function(cb){
        return P.try(function(){
            return env.startServer('s1');
        })
        .then(function(serverProcess){
            return env.stopServer(serverProcess);
        })
        .nodeify(cb);
    });
});
