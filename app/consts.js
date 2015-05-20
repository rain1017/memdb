'use strict';

var exports = {};

exports.collMethods = ['find', 'findOne', 'findById', 'findLocked', 'findOneLocked', 'findByIdLocked',
                    'insert', 'update', 'remove', 'lock', 'findCached'];

exports.connMethods = ['commit', 'rollback', 'flushBackend'];

module.exports = exports;
