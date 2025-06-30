package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE;

@Service
@Slf4j
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryTypeList() {
        String jsonStr = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE);
        if (StrUtil.isNotBlank(jsonStr)) {
            // 缓存命中
            if ("[]".equals(jsonStr)) {
                return Collections.emptyList();
            }
            return JSONUtil.toList(jsonStr, ShopType.class);
        }
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if (typeList != null && !typeList.isEmpty()) {
            // 数据库命中
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE, JSONUtil.toJsonStr(typeList), 30, TimeUnit.MINUTES);
        } else {
            // 数据库也没查到
            log.info("数据库中未查到shop type");
            // 设置空值防止缓存穿透
            // 缓存穿透解决方案：1. 设置默认空值 + TTL 2. 业务侧的非法值校验 3. 布隆过滤器
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE, "[]", 5, TimeUnit.MINUTES);
            return Collections.emptyList();
        }
        return typeList;
    }
}
