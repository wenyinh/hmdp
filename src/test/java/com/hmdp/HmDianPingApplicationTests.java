package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
public class HmDianPingApplicationTests {

    @Autowired
    private IShopService shopService;

    @Autowired
    private CacheClient cacheClient;

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testSaveShopToRedis() {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
    }

    @Test
    public void testRedisson() throws InterruptedException {
        RLock lock = redissonClient.getLock("lock");
        boolean b = lock.tryLock(1, 10, TimeUnit.SECONDS);
        if (b) {
            try {
                System.out.println("执行业务逻辑");
            } finally {
                lock.unlock();
            }
        }
    }

    @Test
    public void loadShopLocationData() {
        // 查询店铺信息
        List<Shop> list = shopService.list();
        // 按照typeId分组
        Map<Long, List<Shop>> shopTypeMap = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 存geo，写入redis
        for (Map.Entry<Long, List<Shop>> entry : shopTypeMap.entrySet()) {
            Long typeId = entry.getKey();
            String key = RedisConstants.SHOP_GEO_KEY + typeId;
            List<Shop> value = entry.getValue();
            for (Shop shop : value) {
                Double x = shop.getX();
                Double y = shop.getY();
                stringRedisTemplate.opsForGeo().add(key, new Point(x, y), String.valueOf(shop.getId()));
            }
        }
    }
}
