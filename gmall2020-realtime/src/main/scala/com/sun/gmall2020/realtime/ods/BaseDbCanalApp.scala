package com.sun.gmall2020.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.sun.gmall2020.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManger}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
 * 通过canal 监控数据库的变化
 */
object BaseDbCanalApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ods_canal").setMaster("local[4]")
    val sc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic: String = "GMALL2020_DB"
    val groupId: String = "gmall_base_db_canal_group"

    //从Kafka读取偏移量
    val offsets: Map[TopicPartition, Long] = OffsetManger.getOffset(topic, groupId)
    //加载数据流
    var dbInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      //从kafka中读取偏移量
      println("获取的offsets偏移量" + offsets.mkString(","))
      dbInputDstream = MyKafkaUtil.getKafkaStream(topic, sc, offsets, groupId)
    } else {
      println("没有偏移量")
      dbInputDstream = MyKafkaUtil.getKafkaStream(topic, sc, groupId)
    }

    //读取偏移量的位置
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordDstream: DStream[ConsumerRecord[String, String]] = dbInputDstream.transform {
      rdd =>
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    //提取出数据
    val jsonObjDstream: DStream[JSONObject] = recordDstream.map {
      record =>
        val jsonStr: String = record.value()
       // println("jsonStr"+jsonStr)
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
    }

    //讲数据写入
    //{"data":[{"id":"22","user_name":"7","tel":"13810001010"}],"database":"gmall0213","es":1595833347000,"id":8,"isDdl":false,"mysqlType":{"id":"bigint(20)","user_name":"varchar(20)","tel":"varchar(20)"},"old":[{"user_name":"6"}],"pkNames":["id"],"sql":"","sqlType":{"id":-5,"user_name":12,"tel":12},"table":"z_user_info","ts":1595833347335,"type":"UPDATE"}
    jsonObjDstream.foreachRDD { rdd =>
      rdd.foreach { jsonObj =>
        val tableName: String = jsonObj.getString("table")
        val optType: String = jsonObj.getString("type")
        val dataArray: JSONArray = jsonObj.getJSONArray("data")
        //println(tableName+":"+optType+":===="+dataArray.toJSONString)
        val topic = "ODS_" + tableName.toUpperCase
        if ((tableName.equals("order_info") && optType.equals("INSERT"))
          || (tableName.equals("order_detail") && optType.equals("INSERT"))
        ) {
            for (i <- 0 to dataArray.size() - 1) {
            val jsonData: String = dataArray.getString(i)
            //发送kafka主题 讲消息发向ods层

           MyKafkaSink.send(topic, jsonData)
          }
        }
      }
      //driver中提交偏移量
      OffsetManger.saveOffset(topic,groupId,ranges)
    }


    sc.start()
    sc.awaitTermination()
  }

}
