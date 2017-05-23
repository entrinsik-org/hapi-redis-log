'use strict';

const Writable = require('stream').Writable;
const redis = require('redis');

class RedisLogStream extends Writable {
    constructor({ connection, name, maxSize = 1000 }) {
        super({ objectMode: true });
        this.client = redis.createClient(connection);
        this.name = name;
        this.maxSize = maxSize;
    }

    _write(chunk, enc, done) {
        this.client.multi()
            .lpush(this.name, chunk)
            .ltrim(this.name, 0, this.maxSize)
            .exec(done);
    }
}

exports.RedisLogStream = RedisLogStream;