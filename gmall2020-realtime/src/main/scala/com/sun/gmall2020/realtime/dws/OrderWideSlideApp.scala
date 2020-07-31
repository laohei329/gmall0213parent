package com.sun.gmall2020.realtime.dws

import java.lang

import com.alibaba.fastjson.JSON
import com.sun.gmall2020.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.sun.gmall2020.realtime.util.{MyKafkaUtil, OffsetManger, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderWideSlideApp {
  def main(args: Array[String]): Unit = {
    //双流  订单主表  订单从表    偏移量 双份
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dws_order_wide_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "DWD_ORDER_INFO"
    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "DWD_ORDER_DETAIL"

    //1   从redis中读取偏移量   （启动执行一次）
    val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManger.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManger.getOffset(orderDetailTopic, orderDetailGroupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
    } else {
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }


    var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
    } else {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }


    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var orderInfoOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    // 1 提取数据 2 分topic
    val orderInfoObjDS: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
      orderInfo
    }
    val orderDetailObjDS: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val jsonStr: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])
      orderDetail
    }
    val orderInfoWindowDS: DStream[OrderInfo] = orderInfoObjDS.window(Seconds(15), Seconds(5))
    val orderDetailWindowDS: DStream[OrderDetail] = orderDetailObjDS.window(Seconds(15), Seconds(5))
    //k-v的形式才能join
    val orderInfoWindowMap: DStream[(Long, OrderInfo)] = orderInfoWindowDS.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWindowMap: DStream[(Long, OrderDetail)] = orderDetailWindowDS.map(orderDetil => (orderDetil.order_id, orderDetil))
    val joinedDS: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowMap.join(orderDetailWindowMap, 4)
    // 去重
    //  数据统一保存到
    // redis ?  type? set   api? sadd   key ? order_join:[orderId]        value ? orderDetailId (skuId也可）  expire : 60*10
    // sadd 返回如果0  过滤掉
    val orderWideDS: DStream[OrderWide] = joinedDS.mapPartitions { tupleItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val widesList: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
      for ((orderId, (orderInfo, orderDetail)) <- tupleItr) {
        val key = "order_join:" + orderId
        val isExisted: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        if (isExisted == 1L) {
          widesList.append(new OrderWide(orderInfo, orderDetail))
        }
      }
      jedis.close()
      widesList.iterator
    }
    orderWideDS.print(1000)
//    orderWideDS.mapPartitions{rdd=>
//      rdd.foreach{orderWide=>
//
//
//      }
//    }




    ssc.start()
    ssc.awaitTermination()

  }
}
