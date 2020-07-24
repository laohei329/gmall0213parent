package com.sun.gmall2020.realtime.util

import java.util.Properties

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool:JedisPool=null;
  def getJedisClient ={
    if (jedisPool==null){
      val properties: Properties = PropertiesUtil.load("config.properties")
      val host: String = properties.getProperty("redis.host")
      val port: Int = properties.getProperty("redis.port").toInt
      val config: JedisPoolConfig = new JedisPoolConfig()
      config.setMaxTotal(100)  //最大连接数
      config.setMaxIdle(20)   //最大空闲
      config.setMinIdle(20)     //最小空闲
      config.setBlockWhenExhausted(true)  //忙碌时是否等待
      config.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      config.setTestOnBorrow(true) //每次获得连接的进行测试
      jedisPool = new JedisPool(config,host,port)
    }
    jedisPool.getResource
  }

}
