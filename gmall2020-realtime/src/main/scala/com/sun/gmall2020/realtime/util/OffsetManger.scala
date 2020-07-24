package com.sun.gmall2020.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManger {
  def getOffset(topic: String, consumerGroup: String) = {
    //redis type:hash key:topic+consummerGroup  value :偏移量
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + consumerGroup
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import collection.JavaConversions._
    if (offsetKey != null && offsetKey.size > 0) {

      val offsetList: List[(String, String)] = offsetMap.toList

      val offsetListForKafka: List[(TopicPartition, Long)] = offsetList.map {
        case (partition, offset) => {
          //主题和分区作为key
          val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
          (topicPartition, offset.toLong)
        }
      }
      val offsetTopicMap: Map[TopicPartition, Long] = offsetListForKafka.toMap
      offsetTopicMap
    } else {
      null
    }
  }

  /**
   *
   * @param topic
   * @param groupId
   * @param offsetRanges 存储偏移量  A topic name and partition number
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    val offsetKey = topic + ":" + groupId
    for (offset <- offsetRanges) {
      val partition: String = offset.partition.toString
      val untilOffset: String = offset.untilOffset.toString
      offsetMap.put(partition, untilOffset)
    }
    val jedis: Jedis = RedisUtil.getJedisClient
    jedis.hmset(offsetKey, offsetMap)
    jedis.close()
  }
}
