package com.sun.gmall2020.realtime.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.sun.gmall2020.realtime.bean.{OrderDetail, OrderInfo}
import com.sun.gmall2020.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManger}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dwd_order_detail_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic = "ODS_ORDER_DETAIL"
    val groupID = "dwd_order_detail_group"
    //读取偏移量
    val offsets: Map[TopicPartition, Long] = OffsetManger.getOffset(topic, groupID)
    var recordInpuDS: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.size > 0) {
      recordInpuDS = MyKafkaUtil.getKafkaStream(topic, ssc, offsets, groupID)
    } else {
      recordInpuDS = MyKafkaUtil.getKafkaStream(topic, ssc, groupID)
    }
    var ranges: Array[OffsetRange]=null
    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val jsonObjDS: DStream[ConsumerRecord[String, String]] = recordInpuDS.transform { rdd =>
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val orderDetailDS: DStream[OrderDetail] = jsonObjDS.map { record =>
      val jsonStr: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])
      orderDetail
    }
    orderDetailDS.print(1000)
    orderDetailDS.foreachRDD{rdd=>
      rdd.foreach{orderDetail=>
        //数据发往kafka
        MyKafkaSink.send("DWD_ORDER_DETAIL",JSON.toJSONString(orderDetail,new SerializeConfig(true)))
      }
     OffsetManger.saveOffset(topic,groupID,ranges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
