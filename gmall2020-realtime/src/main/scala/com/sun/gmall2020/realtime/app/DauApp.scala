package com.sun.gmall2020.realtime.app

import java.lang
import java.text.SimpleDateFormat
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
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dau_group"
    val inputDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("GMALL_START", ssc, groupId)
    //先得到JSONObject
    val jsonObjectDS: DStream[JSONObject] = inputDS.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }
    val startUpObjs: DStream[JSONObject] = jsonObjectDS.mapPartitions {
      jsonObjIter => {
        val objList: List[JSONObject] = jsonObjIter.toList
        println("过滤之前：" + objList.size)
        val startObjList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        val jedis: Jedis = JedisUtil.getJedisClient
        for (obj <- objList) {
          val mid: String = obj.getJSONObject("common").getString("mid")
          val ts: lang.Long = obj.getLong("ts")
          val time: String = new SimpleDateFormat("yyyy-MM-dd").format(ts)
          val backLong: lang.Long = jedis.sadd("dau:" + time, mid)
          if (backLong == 1L) {
            startObjList.append(obj)
          }
        }
        jedis.close()
        println("过滤之后：" + startObjList.size)
        startObjList.toIterator
      }
    }
    startUpObjs.print(1000)

/*
    val startObject: DStream[JSONObject] = jsonObjectDS.transform {
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
*/

    ssc.start()
    ssc.awaitTermination()
  }

}
