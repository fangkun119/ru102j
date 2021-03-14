package com.redislabs.university.RU102J.examples;

import com.redislabs.university.RU102J.HostPort;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class HelloTest {

    @Test
    public void sayHelloBasic() {
        // 创建Jedis对象
        Jedis jedis = new Jedis(HostPort.getRedisHost(), HostPort.getRedisPort());
        if (HostPort.getRedisPassword().length() > 0) {
            jedis.auth(HostPort.getRedisPassword());
        }

        // SET hello world
        // GET hello
        jedis.set("hello", "world");
        String value = jedis.get("hello");

        // 检查结果
        assertThat(value, is("world"));
    }

    @Test
    public void sayHello() {
        // 单线程Use Case
        // 创建Jedis对象
        Jedis jedis = new Jedis(HostPort.getRedisHost(), HostPort.getRedisPort());
        if (HostPort.getRedisPassword().length() > 0) {
            jedis.auth(HostPort.getRedisPassword());
        }

        // 127.0.0.1:6379> SET hello world
        // OK
        // 127.0.0.1:6379> GET hello
        // "world"
        String result = jedis.set("hello", "world"); // 获取返回值
        assertThat(result, is("OK"));
        String value = jedis.get("hello");
        assertThat(value, is("world"));

        // 关闭连接、防止连接泄漏
        jedis.close();
    }

    @Test
    public void sayHelloThreadSafe() {
        // 多线程环境下，使用JedisPool
        JedisPool jedisPool;
        String password = HostPort.getRedisPassword();
        if (password.length() > 0) {
            jedisPool = new JedisPool(new JedisPoolConfig(),
                    HostPort.getRedisHost(), HostPort.getRedisPort(), 2000, password);
        } else {
            jedisPool = new JedisPool(new JedisPoolConfig(),
                    HostPort.getRedisHost(), HostPort.getRedisPort());
        }

        // 从JedisPool来获取Jedis对象，
        // try-with-resource block保证Jedis对象在离开try块时会被归还给Jedis Pool
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set("hello", "world");
            assertThat(result, is("OK"));
            String value = jedis.get("hello");
            assertThat(value, is("world"));
        }

        // 释放Jedis Pool，同时会释放所有Redis链接
        jedisPool.close();
    }
}
