'use strict';

var exports = {};

exports.collMethods = ['find', 'findOne', 'findById',
                    'findReadOnly', 'findOneReadOnly', 'findByIdReadOnly',
                    'insert', 'update', 'remove', 'lock'];

exports.connMethods = ['commit', 'rollback', 'eval', 'flushBackend', '$unload', '$findReadOnly'];

module.exports = exports;
