package com.sun.gmall2020.gmall2020logger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author ：MrSun
 * @date ：Created in 2020/7/15 19:24
 */
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("applog")
    public String applog(@RequestBody JSONObject jsonObject) {
        String jsonString = jsonObject.toJSONString();
        System.out.println(jsonString);
        if (jsonObject.getJSONObject("start") != null) {
            kafkaTemplate.send("GMALL_START", jsonString);
        } else {
            kafkaTemplate.send("GMALL_EVENT", jsonString);
        }
        log.info(jsonString);
        return "success";
    }
}
