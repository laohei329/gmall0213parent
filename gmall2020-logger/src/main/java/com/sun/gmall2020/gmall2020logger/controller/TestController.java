package com.sun.gmall2020.gmall2020logger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.xml.ws.RequestWrapper;

/**
 * @author ：MrSun
 * @date ：Created in 2020/7/15 19:17
 */
@Controller
public class TestController {
    @ResponseBody
    @RequestMapping("testDemo")  //设置的访问的节点时testDemo
    public String testDemo() {
        return "success";
    }
}
