'use strict';

var util = require('util');
var P = require('bluebird');
var redis = P.promisifyAll(require('redis'));
var Logger = require('memdb-logger');

var GlobalEvent = function(opts) {
    opts = opts || {};

    this.shardId = opts.shardId;
    this.config = {
        host : opts.host || '127.0.0.1',
        port : opts.port || 6379,
        db : opts.db || 0,
        options : opts.options || {},
        prefix : opts.prefix || 'ge$',
    };

    this.pub = null;
    this.sub = null;

    this.eventListeners = {}; // {event : listeners}

    this.logger = Logger.getLogger('memdb', __filename, 'shard:' + this.shardId);
};

var proto = GlobalEvent.prototype;

proto.start = function(){
    return P.bind(this)
    .then(function(){
        this.pub = redis.createClient(this.config.port, this.config.host, {retry_max_delay : 10 * 1000});
        this.sub = redis.createClient(this.config.port, this.config.host, {retry_max_delay : 10 * 1000});

        var self = this;
        this.pub.on('error', function(e){
            self.logger.error(e.stack);
        });
        this.sub.on('error', function(e){
            self.logger.error(e.stack);
        });

        return P.all([this.pub.selectAsync(this.config.db),
            this.sub.selectAsync(this.config.db)]);
    })
    .then(function(){
        var self = this;

        this.sub.on('message', function(channel, msg){
            var event = channel.slice(self.config.prefix.length);
            try{
                msg = JSON.parse(msg);
            }
            catch(err){
                self.logger.error('invalid message %s', msg);
            }

            if(!self.eventListeners.hasOwnProperty(event)){
                return P.resolve();
            }

            P.each(self.eventListeners[event], function(listener){
                return P.try(function(){
                    self.logger.debug('onEvent %s %j', event, msg);
                    return listener.apply(null, msg);
                })
                .catch(function(err){
                    self.logger.error(err.stack);
                });
            });
        });

        this.logger.info('globalEvent started %s:%s:%s', this.config.host, this.config.port, this.config.db);
    });
};

proto.stop = function(){
    var self = this;
    this.eventListeners = {};

    return P.all([this.pub.quitAsync(), this.sub.quitAsync()])
    .then(function(){
        self.logger.info('globalEvent stoped');
    });
};

proto.emit = function(event){
    var args = [].slice.call(arguments, 1);

    this.logger.debug('emit %s %j', event, args);

    return this.pub.publishAsync(this._channel(event), JSON.stringify(args));
};

proto.addListener = function(event, func){
    this.logger.debug('addListener %s', event);

    if(typeof(func) !== 'function'){
        throw new Error('not a function');
    }

    if(this.eventListeners.hasOwnProperty(event)){
        this.eventListeners[event].push(func);
        return P.resolve();
    }

    this.eventListeners[event] = [func];
    return this.sub.subscribeAsync(this._channel(event));
};

proto.on = function(event, func){
    return this.addListener(event, func);
};

proto.removeListener = function(event, func){
    this.logger.debug('removeListener %s', event);

    if(!this.eventListeners.hasOwnProperty(event)){
        return P.resolve();
    }

    var listeners = this.eventListeners[event];
    var index = listeners.indexOf(func);
    if(index === -1){
        return P.resolve();
    }

    listeners.splice(index, 1);
    if(listeners.length > 0){
        return P.resolve();
    }

    delete this.eventListeners[event];
    return this.sub.unsubscribeAsync(this._channel(event));
};

proto.removeAllListeners = function(event){
    this.logger.debug('removeAllListeners %s', event);

    if(!this.eventListeners.hasOwnProperty(event)){
        return P.resolve();
    }

    delete this.eventListeners[event];
    return this.sub.unsubscribeAsync(this._channel(event));
};

proto._channel = function(event){
    return this.config.prefix + event;
};

module.exports = GlobalEvent;
