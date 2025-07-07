package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
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

import java.util.List;

@Service
@Slf4j
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Autowired
    private IUserService userService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

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

    private void setBlogLikedStatus(Blog blog) {
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.BLOG_LIKED_KEY + userId;
        Boolean flag = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        blog.setIsLike(Boolean.TRUE.equals(flag));
    }

    private void patchUserInfoForBlog(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
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
        Boolean flag = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        if (Boolean.FALSE.equals(flag)) {
            // 没点赞过
            boolean isSucceed = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSucceed) {
                stringRedisTemplate.opsForSet().add(key, userId.toString());
            }
        } else {
            // 点赞过了
            boolean isSucceed = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSucceed) {
                stringRedisTemplate.opsForSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }
}
