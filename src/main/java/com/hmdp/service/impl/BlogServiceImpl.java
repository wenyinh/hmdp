package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Autowired
    private IUserService userService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IFollowService followService;

    private void patchUserInfoForBlog(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    private void setBlogLikedStatus(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return;
        }
        Long userId = user.getId();
        String key = RedisConstants.BLOG_LIKED_KEY + userId;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Blog queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null) {
            return null;
        }
        patchUserInfoForBlog(blog);
        setBlogLikedStatus(blog);
        return blog;
    }

    @Override
    public List<Blog> quertHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            patchUserInfoForBlog(blog);
            setBlogLikedStatus(blog);
        });
        return records;
    }

    @Override
    public Result likeBlogById(Long id) {
        // 查询当前的登录用户
        Long userId = UserHolder.getUser().getId();
        // 判断当前用户是否点过赞
        String key = RedisConstants.BLOG_LIKED_KEY + userId;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 没点赞过
            boolean isSucceed = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSucceed) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 点赞过了
            boolean isSucceed = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSucceed) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(RedisConstants.BLOG_LIKED_KEY + id, 0, 4);
        List<Long> ids = new ArrayList<>();
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        for (String num : top5) {
            ids.add(Long.valueOf(num));
        }
        String join = StrUtil.join(",", ids);
        List<User> users = userService.query().in("id", ids).last("order by field(id," + join + ")").list();
        List<UserDTO> userDTOS = new ArrayList<>();
        for (User user : users) {
            userDTOS.add(BeanUtil.copyProperties(user, UserDTO.class));
        }
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSaved = save(blog);
        if (!isSaved) {
            return Result.fail("新增Blog失败");
        }
        // 查询笔记作者的粉丝 select * from tb_follow where follow_user_id = userid
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        // 推送笔记ID
        for (Follow follow : follows) {
            Long userId = follow.getUserId();
            String zsetKey = "feed:" + userId;
            stringRedisTemplate.opsForZSet().add(zsetKey, userId.toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> res = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key,
                0, max, offset, 2);
        if (res == null || res.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> blogIds = new ArrayList<>();
        long minTime = 0;
        int newOffset = 1;
        for (ZSetOperations.TypedTuple<String> tuple : res) {
            blogIds.add(Long.valueOf(tuple.getValue()));
            long time = tuple.getScore().longValue();
            if (time == minTime) {
                newOffset++;
            } else {
                minTime = time;
                newOffset = 1;
            }
        }
        String join = StrUtil.join(",", blogIds);
        List<Blog> blogs = query().in("id", blogIds).last("order by field(id," + join + ")").list();
        for (Blog blog : blogs) {
            patchUserInfoForBlog(blog);
            setBlogLikedStatus(blog);
        }
        ScrollResult sr = new ScrollResult();
        sr.setOffset(newOffset);
        sr.setMinTime(minTime);
        sr.setList(blogs);
        return Result.ok(sr);
    }
}
