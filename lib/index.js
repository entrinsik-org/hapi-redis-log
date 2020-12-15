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
        const config = _.get(data, ['config', 'requestResponseFilter']);
        const filterContent = this.tag('content');
        // do not log static information if config is not available
        if (!config && filterContent(data)) return next();

        // passthrough when no filter config or not a 'response' type 'good' log event
        if (!_.keys(config).length || data.event !== 'response') return next(null, data);

        const filters = this.compile(config);
        if (!filters.length) return next(null, data);

        // if any filter for an 'off' setting passes, exclude the data message
        return _.any(filters, filterFn => filterFn(data)) ? next() : next(null, data);
    }

    compile ({ content, viz, api, successes, warnings, errors }) {
        // setting off = add a filter which, if violated, excludes a message from logging
        return _.compact([
            !content && this.tag('content'),
            !viz && this.tag('viz'),
            !api && this.tag('api'),
            !successes && this.statusCode(200, 399),
            !warnings && this.statusCode(400, 499),
            !errors && this.statusCode(500)
        ]);
    }

    tag (tag) {
        return item => _.includes(item.tags, tag);
    }

    statusCode (loInclusive, hiInclusive = 999) {
        return item => item.statusCode >= loInclusive && item.statusCode <= hiInclusive;
    }

}

exports.RequestResponseFilterStream = RequestResponseFilterStream;

exports.register = function (server, opts, next) {
    const { settingsPath } = opts;
    if (settingsPath) {
        server.log(['ent-hapi-redis-log', 'info'], `Request response logging is filtered using settings @request.${settingsPath.join('.')}`);
        server.ext('onPostAuth', (req, reply) => {
            _.set(req.plugins, ['good', 'requestResponseFilter'], _.get(req, settingsPath));
            reply.continue();
        });

    } else {
        server.log(['ent-hapi-redis-log', 'info'], `Request response logging is not filtered`);
    }
    next();
};

exports.register.attributes = {
    pkg: require('../package.json')
};