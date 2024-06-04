const Parse = require('parse/node');
const objectHash = require('object-hash');
const { initializeRedisClient } = require("./redis");

let options = {}
class ParseCache {
    constructor(option = {}, client = null) {
        options = {
            ttl: option.ttl || 1000 * 60 * 5, // Default TTL in milliseconds
        };
        this.cache = client;
    }

    async get(cacheKey, className) {
        const data = await this.cache.get(cacheKey);
        if (data) {
            const parsedData = JSON.parse(data);
            if (Array.isArray(parsedData)) {
                return parsedData.map(item => Parse.Object.fromJSON({...JSON.parse(item), className}));
            } else {
                return Parse.Object.fromJSON({...parsedData, className});
            }
        }
        return null;
    }

    async set(className, cacheKey, data) {
        const jsonData = Array.isArray(data) ? data.map(item => JSON.stringify(item)) : JSON.stringify(data);
        await this.cache.setEx(cacheKey, options.ttl / 1000, JSON.stringify(jsonData));// in seconds
        await this.cache.lPush(className, cacheKey);

    }

    async clear(className) {
        const keys = await this.cache.lRange(className, 0, -1);
        if (keys.length > 0) {
            await this.cache.del(keys);
        }
        await this.cache.del(className);
    }

    generateCacheKey(query, ...args) {
        const key = {
            className: query.className,
            query: query.toJSON(),
            args: args,
        }
        return objectHash(JSON.stringify(key));
    }
}

const fNames = {
    getCache: "get",
    findCache: "find",
    findAllCache: "findAll",
    countCache: "count",
    distinctCache: "distinct",
    aggregateCache: "aggregate",
    firstCache: "first",
    eachBatchCache: "eachBatch",
    eachCache: "each",
    mapCache: "map",
    reduceCache: "reduce",
    filterCache: "filter",
    subscribeCache: "subscribe"
}

async function parseCacheInit(options = {}, redisConfig = { url: "" }) {
    const redisClient = await initializeRedisClient(redisConfig);

    const cache = new ParseCache(options, redisClient);
    const originalSave = Parse.Object.prototype.save;
    const originalSaveAll = Parse.Object.saveAll;
    const originalDestroy = Parse.Object.prototype.destroy;
    const originalDestroyAll = Parse.Object.destroyAll;

    if (options.resetCacheOnSaveAndDestroy) {
        global.Parse.Object.destroyAll = async function (...args) {
            const result = await originalDestroyAll.apply(this, args);
            if (result) {
                // Clear cache
                cache.clear(result[0].className);
                return result;
            }
        }
        global.Parse.Object.prototype.destroy = async function (...args) {
            const result = await originalDestroy.apply(this, args);
            // Clear cache
            cache.clear(this.className);
            return result;
        };
        global.Parse.Object.saveAll = async function (...args) {
            const result = await originalSaveAll.apply(this, args);
            if (result) {
                // Clear cache
                cache.clear(result[0].className);
                return result;
            }
        }
        global.Parse.Object.prototype.save = async function (...args) {
            // const result = await originalSave.apply(this, args);
            const result = await originalSave.call(this, ...args);
            // Clear cache
            cache.clear(this.className);
            return result;
        };
    }

    const cacheMethods = {
        getCache: "get",
        findCache: "find",
        findAllCache: "findAll",
        countCache: "count",
        distinctCache: "distinct",
        aggregateCache: "aggregate",
        firstCache: "first",
        eachBatchCache: "eachBatch",
        eachCache: "each",
        mapCache: "map",
        reduceCache: "reduce",
        filterCache: "filter",
        subscribeCache: "subscribe"
    };

    for (const [methodName, queryMethod] of Object.entries(cacheMethods)) {
        global.Parse.Query.prototype[methodName] = async function (...args) {
            const cacheKey = cache.generateCacheKey(this, ...args, queryMethod);
            let cachedData = await cache.get(cacheKey, this.className);

            if (!cachedData) {
                cachedData = await this[queryMethod](...args);
                if (cachedData)
                    await cache.set(this.className, cacheKey, cachedData);
            }
            return cachedData;
        };
    }

    return cache;
}

module.exports = { parseCacheInit };