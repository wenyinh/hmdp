package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
public class HmDianPingApplicationTests {

    @Autowired
    private IShopService shopService;

    @Autowired
    private CacheClient cacheClient;

    @Autowired
    private RedissonClient redissonClient;

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
}
