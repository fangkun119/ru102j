package com.redislabs.university.RU102J.script;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/* Encapsulates a server-side Lua script to compare
 * a value stored in a hash field and update if
 * greater than or less than a the provided value,
 * as requested.
 */
public class CompareAndUpdateScript {
    private final String sha;
    public static String script = "" +
            "local key = KEYS[1] " +
            "local field = ARGV[1] " +
            "local value = ARGV[2] " +
            "local op = ARGV[3] " +
            "local current = redis.call('hget', key, field) " +
            "if (current == false or current == nil) then " +
            "  redis.call('hset', key, field, value)" +
            "elseif op == '>' then" +
            "  if tonumber(value) > tonumber(current) then" +
            "    redis.call('hset', key, field, value)" +
            "  end " +
            "elseif op == '<' then" +
            "  if tonumber(value) < tonumber(current) then" +
            "    redis.call('hset', key, field, value)" +
            "  end " +
            "end ";

    public CompareAndUpdateScript(JedisPool jedisPool) {
        try (Jedis jedis = jedisPool.getResource()) {
            this.sha = jedis.scriptLoad(script);
        }
    }

    public Response<Object> updateIfGreater(Transaction jedis, String key, String field, Double value) {
        return update(jedis, key, field, value, ScriptOperation.GREATERTHAN);
    }

    public Response<Object> updateIfLess(Transaction jedis, String key, String field, Double value) {
        return update(jedis, key, field, value, ScriptOperation.LESSTHAN);
    }

    private Response<Object> update(Transaction jedis, String key, String field, Double value, ScriptOperation op) {
        if (sha != null) {
            List<String> keys = Collections.singletonList(key);
            List<String> args = Arrays.asList(field, String.valueOf(value), op.getSymbol());
            return jedis.evalsha(sha, keys, args);
        } else {
            return null;
        }
    }
}
