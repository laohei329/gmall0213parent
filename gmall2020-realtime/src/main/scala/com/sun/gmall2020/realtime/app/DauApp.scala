package com.sun.gmall2020.realtime.app

import java.lang
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sun.gmall2020.realtime.util.{JedisUtil, MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer




object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val groupId="dau_group"
    val inputDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("GMALL_START", ssc,groupId)

    //todo 先将log字符串转变成jsonobject
    val JsonObjectDS: DStream[JSONObject] = inputDS.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }
    val startObject: DStream[JSONObject] = JsonObjectDS.transform {
      rdd => {
        rdd.mapPartitions(
          jsonObject => {
            val objectList: List[JSONObject] = jsonObject.toList
            val startList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
            //将数据存储在redis中进行去重
            val jedis: Jedis = JedisUtil.getJedisClient
            for (startObj <- objectList) {
              if (startObj.containsKey("start")) {
                startList += startObj
              }
            }
            startList.toIterator
          }
        )
      }
    }
    startObject

    ssc.start()
    ssc.awaitTermination()
  }

}
