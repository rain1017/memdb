'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var P = require('bluebird');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

var Protocol = function(opts){
    EventEmitter.call(this);

    opts = opts || {};

    this.socket = opts.socket;
    this.socket.setEncoding('utf8');

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

    var ret = this.socket.write(data);
    if(!ret){
        logger.warn('socket.write return false');
    }
};

Protocol.prototype.disconnect = function(){
    this.socket.end();
};

module.exports = Protocol;
