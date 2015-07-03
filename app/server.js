'use strict';

var Database = require('./database');
var memdbLogger = require('memdb-logger');
var net = require('net');
var http = require('http');
var P = require('bluebird');
var Protocol = require('./protocol');

var DEFAULT_PORT = 31017;

exports.start = function(opts){
    var deferred = P.defer();

    var logger = memdbLogger.getLogger('memdb', __filename, 'shard:' + opts.shardId);
    logger.warn('starting %s...', opts.shardId);

    var bind = opts.bind || '0.0.0.0';
    var port = opts.port || DEFAULT_PORT;

    var db = new Database(opts);

    var clients = {}; // {connId : socket}

    var _isShutingDown = false;

    var server = net.createServer(function(socket){

        var connId = db.connect();
        clients[connId] = socket;

        var remoteAddress = socket.remoteAddress;
        var protocol = new Protocol({socket : socket});

        protocol.on('msg', function(msg){
            logger.debug('[conn:%s] %s => %j', connId, remoteAddress, msg);
            var resp = {seq : msg.seq};

            P.try(function(){
                if(msg.method === 'info'){
                    return {
                        connId : connId
                    };
                }
                else{
                    return db.execute(connId, msg.method, msg.args);
                }
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
                logger.debug('[conn:%s] %s <= %j', connId, remoteAddress, resp);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        protocol.on('close', function(){
            P.try(function(){
                return db.disconnect(connId);
            })
            .then(function(){
                delete clients[connId];
                logger.info('[conn:%s] %s disconnected', connId, remoteAddress);
            })
            .catch(function(e){
                logger.error(e.stack);
            });
        });

        protocol.on('error', function(e){
            logger.error(e.stack);
        });

        logger.info('[conn:%s] %s connected', connId, remoteAddress);
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

            Object.keys(clients).forEach(function(connId){
                try{
                    clients[connId].end();
                    clients[connId].destroy();
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
