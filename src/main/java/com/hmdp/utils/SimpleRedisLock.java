package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements Ilock {

    private String name;

    private StringRedisTemplate stringRedisTemplate;

    private static final String LOCK_NAME_PREFIX = "lock:";

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        long id = Thread.currentThread().getId();
        Boolean ok = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_NAME_PREFIX + name, String.valueOf(id), timeoutSec, TimeUnit.MILLISECONDS);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public void unlock() {

    }
}
