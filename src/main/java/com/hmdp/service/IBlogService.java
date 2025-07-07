package com.hmdp.service;

import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

public interface IBlogService extends IService<Blog> {

    Blog queryBlogById(Long id);

    List<Blog> quertHotBlog(Integer current);
}
