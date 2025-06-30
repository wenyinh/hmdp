package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(THREAD_NUM);

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "distributed_lock", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    public void setWithTTL(String key, Object value, Long ttl, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), ttl, timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value, Long ttl, TimeUnit timeUnit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(ttl)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData), ttl, timeUnit);
    }

    public <R, ID> R getWithPassThrough(String prefix, ID id, Class<R> type, Function<ID, R> dbCallBack,
                                        Long ttl, TimeUnit timeUnit) {
        String key = prefix + id.toString();
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }
        R r = dbCallBack.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        setWithTTL(key, r, ttl, timeUnit);
        return r;
    }

    public <R, ID> R getWithLogicalExpire(String prefix, String lockKeyPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbCallBack, Long ttl, TimeUnit timeUnit) {
        String key = prefix + id.toString();
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)) {
            return null;
        }
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 还没过期
            return r;
        }
        String lockKey = lockKeyPrefix + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // 创建线程去更新数据库
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R ret = dbCallBack.apply(id);
                    setWithLogicalExpire(key, ret, ttl, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        // 返回老数据
        return r;
    }

    public <R, ID> R getWithDistributedLock(String cachePrefix, String lockPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbCallBack, Long ttl, TimeUnit timeUnit) throws InterruptedException {
        String key = cachePrefix + id.toString();
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }
        String lockKey = lockPrefix + id;
        R r = null;
        try {
            boolean isLocked = tryLock(lockKey);
            if (!isLocked) {
                Thread.sleep(50);
                return getWithDistributedLock(cachePrefix, lockPrefix, id, type, dbCallBack, ttl, timeUnit);
            }
            r = dbCallBack.apply(id);
            Thread.sleep(200);
            if (r == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), ttl, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }
        return r;
    }
}
