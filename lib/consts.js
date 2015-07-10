'use strict';

var exports = {};

exports.collMethods = ['find', 'findOne', 'findById',
                    'findReadOnly', 'findOneReadOnly', 'findByIdReadOnly',
                    'insert', 'update', 'remove', 'lock'];

exports.connMethods = ['commit', 'rollback', 'eval', 'info', 'resetCounter', 'flushBackend', '$unload', '$findReadOnly'];

module.exports = exports;
