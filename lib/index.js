'use strict';

const stream = require('stream');
const redis = require('redis');
const _ = require('lodash');

class RedisLogStream extends stream.Writable {
    constructor ({ connection, name, maxSize = 1000 }) {
        super({ objectMode: true });
        this.client = redis.createClient(connection);
        this.name = name;
        this.maxSize = maxSize;
    }

    _write (chunk, enc, done) {
        const message = JSON.parse(chunk);
        const tenant = _.get(message, ['config', 'tenant'], 'manager');
        const name = `${this.name}:${tenant}`
        this.client.multi()
            .lpush(name, chunk)
            .ltrim(name, 0, this.maxSize)
            .exec(done);
    }
}

exports.RedisLogStream = RedisLogStream;

class RequestResponseFilterStream extends stream.Transform {
    constructor () {
        super({ objectMode: true });
    }

    _transform (data, enc, next) {
        const config = _.get(data, ['config', 'requestResponseFilter']);
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
        server.log(['ent-hapi-redis-log', 'info'], `Request response logging is filtered using settings @request.server.app.${settingsPath.join('.')}`);
        server.ext('onRequest', (req, reply) => {
            _.set(req.plugins, ['good', 'requestResponseFilter'], _.get(req.server, ['app', ...settingsPath]));
            reply.continue();
        });

        server.ext('onRequest', (req, reply) => {
            _.set(req.plugins, ['good', 'tenant'], req.tenant() || 'manager');
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