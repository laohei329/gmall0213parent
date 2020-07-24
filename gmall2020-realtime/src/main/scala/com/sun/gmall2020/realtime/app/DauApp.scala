package com.sun.gmall2020.realtime.app

import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sun.gmall2020.realtime.bean.DauInfo
import com.sun.gmall2020.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManger, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dau_group"
    val topic = "GMALL_START"
    //先读取偏移量
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManger.getOffset(topic, groupId)
    //讲偏移量传递给kafka  加载数据流（启动执行一次）
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, topic)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, topic)
    }

    //从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRange: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd => {
        offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //写道es中
    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map {
      record => {
        val jsonString: String = record.value()
        val jSONObject: JSONObject = JSON.parseObject(jsonString)
        println("inputGetOffsetDstream===="+jsonString)
        jSONObject
      }
    }

    //对数据进行过滤 然后讲键值对存储到redis
    val jsonObjFilterDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions {
      jsonObjItr => {
        val jsonObjList: List[JSONObject] = jsonObjItr.toList
        println("过滤前" + jsonObjList.size)
        val newJsonObjList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        val redis: Jedis = RedisUtil.getJedisClient
        for (jsonObj <- jsonObjList) {
          val mid = jsonObj.getJSONObject("common").getString("mid")
          val ts: lang.Long = jsonObj.getLong("ts")
          val time: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
          val key = "dau:" + time
          val long: lang.Long = redis.sadd(key, mid)
          if (long == 1L) {
            newJsonObjList.append(jsonObj)
          }
        }
        redis.close()
        println("过滤后" + newJsonObjList.size)
        newJsonObjList.iterator
      }
    }


    //写入到ES中
    jsonObjFilterDstream.foreachRDD { rdd =>
      rdd.foreachPartition{
        jsonObjItr=>
          val jsonObjList: List[JSONObject] = jsonObjItr.toList
          val formattor: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
          val dauWithIdList: List[(DauInfo, String)] = jsonObjList.map {
            obj =>
              val common: JSONObject = obj.getJSONObject("common")
              //获取日期 、小时、分钟
              val ts: lang.Long = obj.getLong("ts")
              val dateTimeString: String = formattor.format(new Date(ts))
              val dateTimeArr: Array[String] = dateTimeString.split(" ")
              val dt: String = dateTimeArr(0)
              val time: String = dateTimeArr(1)
              val timeArr: Array[String] = time.split(":")
              val hr: String = timeArr(0)
              val mi: String = timeArr(1)

              val dauInfo = DauInfo(common.getString("mid"),
                common.getString("uid"),
                common.getString("ar"),
                common.getString("ch"),
                common.getString("vc"),
                dt, hr, mi, ts
              )
              (dauInfo, dauInfo.mid)

          }
          val today= new SimpleDateFormat("yyyyMMdd").format(new Date())
          MyEsUtil.bulkSave(dauWithIdList,"gmall_dau_info_"+today)

      }
      OffsetManger.saveOffset(topic,groupId,offsetRange)// 要在driver中执行 周期性 每批执行一次


    }






    ssc.start()
    ssc.awaitTermination()
  }

}
