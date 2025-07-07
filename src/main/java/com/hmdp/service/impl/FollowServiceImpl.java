package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IUserService userService;

    @Override
    public Result follow(Long id, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = "follow:" + userId.toString();
        if (Boolean.TRUE.equals(isFollow)) {
            // 关注
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(id);
            boolean succeed = save(follow);
            if (succeed) {
                stringRedisTemplate.opsForSet().add(key, id.toString());
            }
        } else {
            // 取关
            boolean succeed = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", id));
            if (succeed) {
                stringRedisTemplate.opsForSet().remove(key, id.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        Integer count = query().eq("user_id", UserHolder.getUser().getId()).eq("follow_user_id", followUserId).count();
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long followUserId) {
        UserDTO userDTO = UserHolder.getUser();
        String key = "follow:" + userDTO.getId().toString();
        String key2 = "follow:" + followUserId.toString();
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> intersectIds = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<User> users = userService.listByIds(intersectIds);
        List<UserDTO> userDTOs = users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(userDTOs);
    }
}
