package com.hmdp;

import com.hmdp.service.IShopService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class HmDianPingApplicationTests {

    @Autowired
    private IShopService shopService;

    @Test
    public void testSaveShopToRedis() {
        shopService.saveShopToRedis(1L, 10L);
    }
}
