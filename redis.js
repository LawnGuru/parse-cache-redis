const Redis = require('ioredis');

async function initializeRedisClient(redisConfig) {
    if (!redisConfig) {
        throw new Error('Redis configuration is required');
    }

    try {
        if (redisConfig.cluster) {
            const cluster = new Redis.Cluster( [{ host: redisConfig.host, port: redisConfig.port }],
              {
                dnsLookup: (address, callback) => callback(null, address),
                redisOptions: {
                  tls: {},
                },
              });

            cluster.on('error', (err) => console.error('Redis Cluster Error', err));
            
            console.log('Connected to Redis Cluster successfully!');
            return cluster;
        } else {
            const redisClient = new Redis(redisConfig);

            redisClient.on('error', (err) => console.error('Redis Error:', err));
            redisClient.on('connect', () => console.log('Connected to Redis successfully!'));

            return redisClient;
        }
    } catch (error) {
        console.error('Failed to initialize Redis client:', error);
        throw error;
    }
}

module.exports = { initializeRedisClient };