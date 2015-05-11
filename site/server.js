'use strict';

var nodeStatic = require('node-static');

var file = new nodeStatic.Server('.');

require('http').createServer(function (request, response) {
    request.addListener('end', function () {
        file.serve(request, response);
    }).resume();
}).listen(8080);
