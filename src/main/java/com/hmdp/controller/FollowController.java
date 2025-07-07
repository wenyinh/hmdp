package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/follow")
public class FollowController {

    @Autowired
    private IFollowService followService;

    @PutMapping("/{id}/{isFollow}")
    public Result followOrUnfollow(@PathVariable("id") Long id, @PathVariable("isFollow") Boolean isFollow) {
        return followService.follow(id, isFollow);
    }

    /**
     * 判断当前用户有没有follow别人
     * @param followUserId follow的目标用户ID
     * @return 是否follow
     */
    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable("id") Long followUserId) {
        return followService.isFollow(followUserId);
    }

    /**
     * 获取当前用户与followUserId对应的用户是否有共同关注
     * @param followUserId 当前用户的关注用户，需要查找当前用户和这个用户的共同关注列表
     * @return 当前用户和followUserId对应的用户的共同关注列表
     */
    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long followUserId) {
        return followService.followCommons(followUserId);
    }
}
