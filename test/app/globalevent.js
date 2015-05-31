'use strict';

var P = require('bluebird');
var should = require('should');
var env = require('../env');
var GlobalEvent = require('../../app/globalevent');

var logger = require('memdb-logger').getLogger('test', __filename);

describe('globalevent test', function(){

    it('event', function(cb){
        var ge1 = new GlobalEvent({
            host : env.config.shards.s1.event.host,
            port : env.config.shards.s1.event.port,
            db : env.config.shards.s1.event.db,
            shardId : 's1',
        });

        var ge2 = new GlobalEvent({
            host : env.config.shards.s2.event.host,
            port : env.config.shards.s2.event.port,
            db : env.config.shards.s2.event.db,
            shardId : 's2',
        });

        var receives = [];

        var handler11 = function(){
            return P.try(function(){
                receives.push('11');
            });
        };
        var handler12 = function(){
            receives.push('12');
        };
        var handler21 = function(){
            receives.push('21');
        };

        return P.all([ge1.start(), ge2.start()])
        .then(function(){
            return ge1.on('e1', handler11);
        })
        .then(function(){
            return ge1.on('e1', handler12);
        })
        .then(function(){
            return ge2.on('e1', handler21);
        })
        .then(function(){
            return ge1.emit('e1', 'from', 'ge1');
        })
        .delay(50)
        .then(function(){
            receives.sort().should.eql(['11', '12', '21']);
            receives = [];
        })
        .then(function(){
            return ge2.emit('e1', 'from', 'ge2');
        })
        .delay(50)
        .then(function(){
            receives.sort().should.eql(['11', '12', '21']);
            receives = [];
        })
        .then(function(){
            return ge1.removeListener('e1', handler12);
        })
        .then(function(){
            return ge2.emit('e1', 'from', 'ge2');
        })
        .delay(50)
        .then(function(){
            receives.sort().should.eql(['11', '21']);
            receives = [];
        })
        .then(function(){
            return ge1.removeAllListeners('e1');
        })
        .then(function(){
            return ge1.emit('e1', 'from', 'ge1');
        })
        .delay(50)
        .then(function(){
            receives.sort().should.eql(['21']);
            receives = [];
        })
        .then(function(){
            return ge2.removeAllListeners('e1');
        })
        .then(function(){
            return ge1.emit('e1', 'from', 'ge1');
        })
        .delay(50)
        .then(function(){
            receives.sort().should.eql([]);
            receives = [];
        })
        .then(function(){
            return P.all([ge1.stop(), ge2.stop()]);
        })
        .nodeify(cb);
    });
});
