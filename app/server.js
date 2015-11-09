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

var Database = require('./database');
var memdbLogger = require('memdb-logger');
var net = require('net');
var http = require('http');
var P = require('bluebird');
var uuid = require('node-uuid');
var Protocol = require('./protocol');
var utils = require('./utils');
var consts = require('./consts');

var DEFAULT_PORT = 31017;

exports.start = function(opts){
    var deferred = P.defer();

    var logger = memdbLogger.getLogger('memdb', __filename, 'shard:' + opts.shardId);
    logger.warn('starting %s...', opts.shardId);

    var bind = opts.bind || '0.0.0.0';
    var port = opts.port || DEFAULT_PORT;

    var db = new Database(opts);

    var sockets = utils.forceHashMap();

    var _isShutingDown = false;

    var server = net.createServer(function(socket){

        var clientId = uuid.v4();
        sockets[clientId] = socket;

        var connIds = utils.forceHashMap();
        var remoteAddress = socket.remoteAddress;
        var protocol = new Protocol({socket : socket});

        protocol.on('msg', function(msg){
            logger.debug('[conn:%s] %s => %j', msg.connId, remoteAddress, msg);
            var resp = {seq : msg.seq};

            P.try(function(){
                if(msg.method === 'connect'){
                    var clientVersion = msg.args[0];
                    if(parseFloat(clientVersion) < parseFloat(consts.minClientVersion)){
                        throw new Error('client version not supported, please upgrade');
                    }
                    var connId = db.connect().connId;
                    connIds[connId] = true;
                    return {
                        connId : connId,
                    };
                }
                if(!msg.connId){
                    throw new Error('connId is required');
                }
                if(msg.method === 'disconnect'){
                    return db.disconnect(msg.connId)
                    .then(function(){
                        delete connIds[msg.connId];
                    });
                }
                return db.execute(msg.connId, msg.method, msg.args);
            })
            .then(function(ret){
                resp.err = null;
                resp.data = ret;
            }, function(err){
                resp.err = {
                    message : err.message,
                    stack : err.stack,
                };
                resp.data = null;
            })
            .then(function(){
                protocol.send(resp);
                logger.debug('[conn:%s] %s <= %j', msg.connId, remoteAddress, resp);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        protocol.on('close', function(){
            P.map(Object.keys(connIds), function(connId){
                return db.disconnect(connId);
            })
            .then(function(){
                connIds = utils.forceHashMap();
                delete sockets[clientId];
                logger.info('client %s disconnected', remoteAddress);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        protocol.on('error', function(e){
            logger.error(e.stack);
        });

        logger.info('client %s connected', remoteAddress);
    });

    server.on('error', function(err){
        logger.error(err.stack);

        if(!deferred.isResolved()){
            deferred.reject(err);
        }
    });

    P.try(function(){
        return P.promisify(server.listen, server)(port, bind);
    })
    .then(function(){
        return db.start();
    })
    .then(function(){
        logger.warn('server started on %s:%s', bind, port);
        deferred.resolve();
    })
    .catch(function(err){
        logger.error(err.stack);
        deferred.reject(err);
    });

    var shutdown = function(){
        logger.warn('receive shutdown signal');

        if(_isShutingDown){
            return;
        }
        _isShutingDown = true;

        return P.try(function(){
            var deferred = P.defer();

            server.once('close', function(){
                logger.debug('on server close');
                deferred.resolve();
            });

            server.close();

            Object.keys(sockets).forEach(function(id){
                try{
                    sockets[id].end();
                    sockets[id].destroy();
                }
                catch(e){
                    logger.error(e.stack);
                }
            });

            return deferred.promise;
        })
        .then(function(){
            return db.stop();
        })
        .catch(function(e){
            logger.error(e.stack);
        })
        .finally(function(){
            logger.warn('server closed');
            memdbLogger.shutdown(function(){
                process.exit(0);
            });
        });
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);

    process.on('uncaughtException', function(err) {
        logger.error('Uncaught exception: %s', err.stack);
    });

    return deferred.promise;
};
