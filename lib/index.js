'use strict';

const stream = require('stream');
const redis = require('redis');
const _ = require('lodash');

class RedisLogStream extends stream.Writable {
    constructor ({ connection, name, maxSize = 1000, redisKey }) {
        super({ objectMode: true });
        this.client = redis.createClient(connection);
        this.name = _.isFunction(redisKey) ? redisKey : _.constant(name);
        this.maxSize = maxSize;
    }

    _write (chunk, enc, done) {
        const name = this.name(chunk);
        chunk = _.isString(chunk) ? chunk : JSON.stringify(chunk);
        this.client.multi()
            .lpush(name, chunk)
            .ltrim(name, 0, this.maxSize)
            .exec(done);
    }
}

class RedisPubSubStream extends stream.Writable {
    constructor ({ connection, channel }) {
        super({ objectMode: true });
        this.client = redis.createClient(connection);
        this.channel = channel;
    }

    _write (chunk, enc, done) {
        if (this.channel) {
            chunk = _.isString(chunk) ? chunk : JSON.stringify(chunk);
            this.client.publish(this.channel, chunk, done);
        } else {
            done();
        }
    }
}

exports.RedisLogStream = RedisLogStream;
exports.RedisPubSubStream = RedisPubSubStream;

class RequestResponseFilterStream extends stream.Transform {
    constructor () {
        super({ objectMode: true });
    }

    _transform (data, enc, next) {
        return next(null, data);
    }

}

exports.RequestResponseFilterStream = RequestResponseFilterStream;

exports.register = function (server, opts, next) {
    server.log(['ent-hapi-redis-log', 'info'], `Logs are being pushed to Redis.`);
    next();
};

exports.register.attributes = {
    pkg: require('../package.json')
};
