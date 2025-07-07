package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
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
}
