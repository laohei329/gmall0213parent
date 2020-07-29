package com.sun.gmall2020.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sun.gmall2020.realtime.bean.ProvinceInfo
import com.sun.gmall2020.realtime.util.{MyKafkaUtil, OffsetManger}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
object DimBaseProvinceApp {
  def main(args: Array[String]): Unit = {

    //1 从ods层(kafka) 获得对应维表数据  //2 偏移量后置 幂等
    //2 数据转换 case class
    //3 保存到hbase(phoenix)
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_base_province_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dim_base_province_group"
    val topic = "ODS_BASE_PROVINCE";
    //1.从redis中读取数据偏移量
    val offests: Map[TopicPartition, Long] = OffsetManger.getOffset(topic, groupId)
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    if (offests != null && offests.nonEmpty) {
      println("读取数据偏移量" + offests.mkString(","))
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offests, groupId)
    } else {
      println("没有数据偏移量")
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId);
    }
    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var ranges: Array[OffsetRange] = null
    val provinceDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd =>
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val provinceInfoDstream: DStream[ProvinceInfo] = provinceDstream.map { record =>
      val jsonStr: String = record.value()
      val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
      provinceInfo
    }
    provinceInfoDstream.print(1000)
    //4 写入phoenix
    provinceInfoDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0213_PROVINCE_INFO",
        Seq("ID", "NAME", "AREA_CODE","ISO_CODE","ISO_3166_2"),
        new Configuration,
        Some("hadoop202,hadoop203,hadoop204:2181"))
      OffsetManger.saveOffset(topic,groupId,ranges)
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
