'use strict';

var P = require('bluebird');
var AsyncLock = require('async-lock');
var Client = require('./client');
var logger = require('memdb-logger').getLogger('memdb-client', __filename);

var clients = {}; // {'host:port' : client}
var connectLock = new AsyncLock();

exports.getClient = function(host, port){
    var key = host + ':' + port;

    return connectLock.acquire(key, function(){
        if(clients[key]){
            return clients[key];
        }

        var client = new Client();
        client.setMaxListeners(1025);

        return P.try(function(){
            return client.connect(host, port);
        })
        .then(function(){
            clients[key] = client;

            client.on('close', function(){
                delete clients[key];
            });
            return client;
        });
    });
};
