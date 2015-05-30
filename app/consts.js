'use strict';

var exports = {};

exports.collMethods = ['find', 'findOne', 'findById',
                    'findReadOnly', 'findOneReadOnly', 'findByIdReadOnly',
                    'insert', 'update', 'remove', 'lock'];

exports.connMethods = ['commit', 'rollback', 'flushBackend'];

module.exports = exports;
