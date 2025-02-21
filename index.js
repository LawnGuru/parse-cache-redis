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

    async get(cacheKey) {
        const data = await this.cache.get(cacheKey);
        if (data) {
            const parsedData = JSON.parse(data);
            if (Array.isArray(parsedData)) {
                return parsedData.map(item => Parse.Object.fromJSON(item));
            } else {
                return Parse.Object.fromJSON(parsedData);
            }
        }
        return null;
    }

    async set(className, cacheKey, data) {
        const jsonData = Array.isArray(data) ? data.map(item => this.prepareToCache(item)) : this.prepareToCache(data);
        await this.cache.set(cacheKey, JSON.stringify(jsonData), 'EX', options.ttl / 1000);// in seconds
        await this.cache.lpush(className, cacheKey);

    }

    async clear(className) {
        const keys = await this.cache.lrange(className, 0, -1);
        if (keys.length > 0) {
            await this.cache.del(keys);
        }
        await this.cache.del(className);
    }

    prepareToCache(object) {
        const json = object.toJSON();
        json.className = object.className;
        if (object.createdAt) json.createdAt = object.createdAt.toISOString();
        if (object.updatedAt) json.updatedAt = object.updatedAt.toISOString();
        if (object.ACL) json.ACL = object.getACL().toJSON();
        return json;
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

async function parseCacheInit(options = {}, redisConfig) {
    const redisClient = await initializeRedisClient(redisConfig);

    const cache = new ParseCache(options, redisClient);
    const originalSave = Parse.Object.prototype.save;
    const originalDestroy = Parse.Object.prototype.destroy;
    const originalSaveAll = Parse.Object.saveAll;
    const originalDestroyAll = Parse.Object.destroyAll;

    global.Parse.Object.prototype.destroy = async function (...args) {
        if (this.get('Customer')) {
            const listKey = `${this.className}${this.get('Customer').id}`         
            cache.clear(listKey);
        }
        if (this.get('Provider')) {
            const listKey = `${this.className}${this.get('Provider').id}`         
            cache.clear(listKey);
        }
        const result = await originalDestroy.apply(this, args);
        return result;
    };
    
    global.Parse.Object.prototype.save = async function (...args) {
        // Clear cache
        if (this.get('Customer')) {
            const listKey = `${this.className}${this.get('Customer').id}`         
            cache.clear(listKey);
        }
        if (this.get('Provider')) {
            const listKey = `${this.className}${this.get('Provider').id}`         
            cache.clear(listKey);
        }        
        const result = await originalSave.call(this, ...args);
        return result;
    };

    global.Parse.Object.destroyAll = async function (...args) {
        const result = await originalDestroyAll.apply(this, args);
        if (result) {
            // Clear cache
            try {
                result.map(obj => {
                    if (obj.get('Customer') && obj.get('Customer').id) {
                        const listKey = `${obj.className}${obj.get('Customer').id}`         
                        cache.clear(listKey);
                    }
                    if (obj.get('Provider') && obj.get('Provider').id) {
                        const listKey = `${obj.className}${obj.get('Provider').id}`         
                        cache.clear(listKey);
                    }
                })
            } catch (e) {

            }
            return result;
        }
    }

    global.Parse.Object.saveAll = async function (...args) {
        const result = await originalSaveAll.apply(this, args);
        if (result) {
            // Clear cache
            try {

                result.map(obj => {
                    if (obj.get('Customer') && obj.get('Customer').id) {
                        const listKey = `${obj.className}${obj.get('Customer').id}`         
                        cache.clear(listKey);
                    }
                    if (obj.get('Provider') && obj.get('Provider').id) {
                        const listKey = `${obj.className}${obj.get('Provider').id}`         
                        cache.clear(listKey);
                    }
                })

            } catch(e) {

            }
            return result;
        }
    }


    // clear cache manually
    global.Parse.Query.cleanCache = async function (className, userId) {
        const listKey = `${className}${userId}`         
        cache.clear(listKey);
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
    }


    for (const [methodName, queryMethod] of Object.entries(cacheMethods)) {
        global.Parse.Query.prototype[methodName] = async function (...args) {
            const listKey = `${this.className}${args[1].user}`
            const cacheKey = cache.generateCacheKey(this, ...args, queryMethod);
            let cachedData = await cache.get(cacheKey, listKey);

            if (!cachedData) {
                cachedData = await this[queryMethod](...args);
                if (cachedData)
                    await cache.set(listKey, cacheKey, cachedData);
            }
            return cachedData;
        };
    }

    return cache;
}

module.exports = { parseCacheInit };