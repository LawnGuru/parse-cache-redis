const Parse = require('parse/node');
const objectHash = require('object-hash');
const { initializeRedisClient } = require("./redis");

let options = {}
class ParseCache {
    constructor(option = {}, client = null) {
        options = {
            max: option.max || 500,
            maxSize: option.maxSize || 5000,
            ttl: option.ttl || 1000 * 60 * 5,
            allowStale: option.allowStale || false,
            updateAgeOnGet: option.updateAgeOnGet || false,
            updateAgeOnHas: option.updateAgeOnHas || false,
            sizeCalculation: (value, key) => {
                return 1
            },
            resetCacheOnSaveAndDestroy: option.resetCacheOnSaveAndDestroy || false
        };
        this.cache = client;
    }

    async get(cacheKey) {
        const data = await this.cache.get(cacheKey);
        return data ? JSON.parse(data) : null;
    }

    async set(className, cacheKey, data) {
        await this.cache.set(cacheKey, JSON.stringify(data));
        await this.cache.lpush(className, cacheKey);
    }

    async clear(className) {
        const keys = await this.cache.lrange(className, 0, -1);
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
            let cachedData = await cache.get(cacheKey);

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