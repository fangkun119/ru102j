package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZonedDateTime;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            // Key的格式：limiter:windowsize_ms:name:maxHits
            // 例如：limiter:500:get_sites:50
            String key = KeyHelper.getKey("limiter:" + windowSizeMS + ":" + name + ":" + maxHits);
            long now = ZonedDateTime.now().toInstant().toEpochMilli();
            // 执行操作
            Transaction t = jedis.multi();
            String member = now + "-" + Math.random(); 		// 加一个随机数防止被去重
            t.zadd(key, now, member); 						// score是当前时间的毫秒数
            t.zremrangeByScore(key, 0, now - windowSizeMS); // 删除滑到窗口外的member
            Response hits = t.zcard(key);					// 计算当前还有多少个memeber
            t.exec();
            // 检查是否超过rate limiter限制
            if (null != hits.get() && (Long)hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
