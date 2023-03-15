package com.gotocompany.depot.redis.client;


import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.ttl.RedisTTLFactory;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;

/**
 * Redis client factory.
 */
public class RedisClientFactory {

    private static final String DELIMITER = ",";

    public static RedisClient getClient(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkConfig.getSinkRedisDeploymentType();
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        return RedisSinkDeploymentType.CLUSTER.equals(redisSinkDeploymentType)
                ? getRedisClusterClient(redisTTL, redisSinkConfig, statsDReporter)
                : getRedisStandaloneClient(redisTTL, redisSinkConfig, statsDReporter);
    }

    private static RedisStandaloneClient getRedisStandaloneClient(RedisTtl redisTTL, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        HostAndPort hostAndPort;
        try {
            hostAndPort = HostAndPort.parseString(StringUtils.trim(redisSinkConfig.getSinkRedisUrls()));
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format("Invalid url for redis standalone: %s", redisSinkConfig.getSinkRedisUrls()));
        }
        Jedis jedis = new Jedis(hostAndPort);
        return new RedisStandaloneClient(new Instrumentation(statsDReporter, RedisStandaloneClient.class), redisTTL, jedis);
    }

    private static RedisClusterClient getRedisClusterClient(RedisTtl redisTTL, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        String[] redisUrls = redisSinkConfig.getSinkRedisUrls().split(DELIMITER);
        HashSet<HostAndPort> nodes = new HashSet<>();
        try {
            for (String redisUrl : redisUrls) {
                nodes.add(HostAndPort.parseString(StringUtils.trim(redisUrl)));
            }
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format("Invalid url(s) for redis cluster: %s", redisSinkConfig.getSinkRedisUrls()));
        }
        JedisCluster jedisCluster = new JedisCluster(nodes);
        return new RedisClusterClient(new Instrumentation(statsDReporter, RedisClusterClient.class), redisTTL, jedisCluster);
    }
}
