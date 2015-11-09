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

var P = require('bluebird');
var util = require('util');
var net = require('net');
var EventEmitter = require('events').EventEmitter;
var Protocol = require('./protocol');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

var Client = function(){
    EventEmitter.call(this);

    this.protocol = null;
    this.seq = 1;
    this.requests = {}; //{seq : deferred}
    this.domains = {}; //{seq : domain} saved domains

    this.disconnectDeferred = null;
};

util.inherits(Client, EventEmitter);

var proto = Client.prototype;

proto.connect = function(host, port){
    if(!!this.protocol){
        throw new Error('connect already called');
    }

    var self = this;
    logger.debug('start connect to %s:%s', host, port);

    var connectDeferred = P.defer();

    var socket = net.createConnection(port, host);

    this.protocol = new Protocol({socket : socket});

    this.protocol.once('connect', function(){
        logger.info('connected to %s:%s', host, port);
        connectDeferred.resolve();
    });

    this.protocol.on('close', function(){
        logger.info('disconnected from %s:%s', host, port);

        // reject all remaining requests
        for(var seq in self.requests){
            process.domain = self.domains[seq];
            self.requests[seq].reject(new Error('connection closed'));
        }
        self.requests = {};
        self.domains = {};

        // Server will not disconnect if the client process exit immediately
        // So delay resolve promise
        if(self.disconnectDeferred){
            setTimeout(function(){
                self.disconnectDeferred.resolve();
            }, 1);
        }
        self.protocol = null;

        self.emit('close');
    });

    this.protocol.on('msg', function(msg){
        var request = self.requests[msg.seq];
        if(!request){
            return;
        }

        // restore saved domain
        process.domain = self.domains[msg.seq];

        if(!msg.err){
            logger.info('%s:%s => %j', host, port, msg);
            request.resolve(msg.data);
        }
        else{
            logger.error('%s:%s => %j', host, port, msg);
            request.reject(msg.err);
        }
        delete self.requests[msg.seq];
        delete self.domains[msg.seq];
    });

    this.protocol.on('error', function(err){
        if(!connectDeferred.isResolved()){
            connectDeferred.reject(err);
        }
        // Reject all pending requests
        Object.keys(self.requests).forEach(function(seq){
            // restore saved domain
            process.domain = self.domains[seq];
            self.requests[seq].reject(err);

            delete self.domains[seq];
            delete self.requests[seq];
        });
    });

    this.protocol.on('timeout', function(){
        self.disconnect();
    });

    return connectDeferred.promise;
};

proto.disconnect = function(){
    if(!this.protocol){
        return;
    }

    this.disconnectDeferred = P.defer();
    this.protocol.disconnect();

    return this.disconnectDeferred.promise;
};

proto.request = function(connId, method, args){
    if(!this.protocol){
        throw new Error('not connected');
    }

    var seq = this.seq++;

    var deferred = P.defer();
    this.requests[seq] = deferred;

    var msg = {
        seq : seq,
        connId : connId,
        method : method,
        args : args,
    };

    this.protocol.send(msg);

    // save domain
    this.domains[seq] = process.domain;

    logger.info('%s:%s <= %j', this.protocol.socket.remoteAddress, this.protocol.socket.remotePort, msg);

    return deferred.promise;
};

module.exports = Client;
