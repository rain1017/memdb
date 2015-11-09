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

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var P = require('bluebird');
var logger = require('memdb-logger').getLogger('memdb', __filename);

var DEFAULT_MAX_MSG_LENGTH = 1024 * 1024;

var Protocol = function(opts){
    EventEmitter.call(this);

    opts = opts || {};

    this.socket = opts.socket;
    this.socket.setEncoding('utf8');

    this.maxMsgLength = opts.maxMsgLength || DEFAULT_MAX_MSG_LENGTH;

    this.remainLine = '';

    var self = this;
    this.socket.on('data', function(data){
        // message is json encoded and splited by '\n'
        var lines = data.split('\n');
        for(var i=0; i<lines.length - 1; i++){
            try{
                var msg = '';
                if(i === 0){
                    msg = JSON.parse(self.remainLine + lines[i]);
                    self.remainLine = '';
                }
                else{
                    msg = JSON.parse(lines[i]);
                }
                self.emit('msg', msg);
            }
            catch(err){
                logger.error(err.stack);
            }
        }
        self.remainLine = lines[lines.length - 1];
    });

    this.socket.on('close', function(hadError){
        self.emit('close', hadError);
    });

    this.socket.on('connect', function(){
        self.emit('connect');
    });

    this.socket.on('error', function(err){
        self.emit('error', err);
    });

    this.socket.on('timeout', function(){
        self.emit('timeout');
    });
};

util.inherits(Protocol, EventEmitter);

Protocol.prototype.send = function(msg){
    var data = JSON.stringify(msg) + '\n';
    if(data.length > this.maxMsgLength){
        throw new Error('msg length exceed limit');
    }

    var ret = this.socket.write(data);
    if(!ret){
        logger.warn('socket.write return false');
    }
};

Protocol.prototype.disconnect = function(){
    this.socket.end();
};

module.exports = Protocol;
