package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.springframework.stereotype.Service;

@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Override
    public Result follow(Long id, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        if (Boolean.TRUE.equals(isFollow)) {
            // 关注
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(id);
            save(follow);
        } else {
            // 取关
            remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", id));
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        Integer count = query().eq("user_id", UserHolder.getUser().getId()).eq("follow_user_id", followUserId).count();
        return Result.ok(count > 0);
    }
}
