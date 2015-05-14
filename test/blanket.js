'user strict';

var blanket = require('blanket');
var path = require('path');

var appDir = path.join(__dirname, '../app');
var libDir = path.join(__dirname, '../lib');

blanket({
    pattern: [appDir, libDir]
});
