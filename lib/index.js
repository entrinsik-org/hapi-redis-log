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

        // passthrough when no filter config
        if (!_.keys(config).length) return next(null, data);

        const filters = this.compile(config);
        if (!filters.length) return next(null, data);

        // if any filter for an 'off' setting passes, exclude the data message
        return (_.any(filters, filterFn => filterFn(data))) ? next() : next(null, data);
    }

    compile ({ content, viz, api, successes, warnings, errors, trace, warn, error, debug, info }) {
        // setting off = add a filter which, if violated, excludes a message from logging
        return _.compact([
            !content && this.tag('content'),
            !viz && this.tag('viz'),
            !api && this.tag('api'),
            !successes && this.statusCode(200, 399),
            !warnings && this.statusCode(400, 499),
            !errors && this.statusCode(500),
            !trace && this.tag('trace'),
            !warn && this.tag('warn'),
            !error && this.tag('error'),
            !debug && this.tag('debug'),
            !info && this.tag('info')
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
    const { settingsPath, logSettingsPath } = opts;
    const logSettingValues = {
        trace: { trace: true, debug: true, info: true, warn: true, error: true },
        debug: { trace: false, debug: true, info: true, warn: true, error: true },
        info: { trace: false, debug: false, info: true, warn: true, error: true },
        warn: { trace: false, debug: false, info: false, warn: true, error: true },
        error: { trace: false, debug: false, info: false, warn: false, error: true }
    };
    if (settingsPath || logSettingsPath) {
        server.log(['ent-hapi-redis-log', 'info'], `Request response logging is filtered using settings @request.${settingsPath.join('.')} and @request.${logSettingsPath.join('.')}`);
        server.ext('onPostAuth', (req, reply) => {
            let settings = _.get(req, settingsPath);
            let logString = _.get(req, logSettingsPath);
            let logLevelSettings = logSettingValues[logString];
            _.set(req.plugins, ['good', 'requestResponseFilter'], _.merge(settings, logLevelSettings));

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
