const redis = require('redis');
async function initializeRedisClient(redisConfig) {
    // read the Redis connection URL from the envs
    if (redisConfig) {
      // create the Redis client object
      const redisClient = redis.createClient({ url: redisConfig.url }).on("error", (e) => {
        console.error(`Failed to create the Redis client with error:`);
        console.error(e);
      });

      try {
        // connect to the Redis server
        await redisClient.connect();
        console.log(`Connected to Redis successfully!`);
        return redisClient;
      } catch (e) {
        console.error(`Connection to Redis failed with error:`);
        console.error(e);
      }
    }
}

exports.initializeRedisClient = initializeRedisClient;