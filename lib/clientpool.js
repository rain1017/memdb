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
