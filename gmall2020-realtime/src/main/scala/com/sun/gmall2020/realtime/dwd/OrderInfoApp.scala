package com.sun.gmall2020.realtime.dwd

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.sun.gmall2020.realtime.bean.{OrderInfo, UserState}
import com.sun.gmall2020.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManger, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast

object OrderInfoApp {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_info")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic = "ODS_ORDER_INFO"
    val groupId = "dwd_order_info_group"

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
    val jsonObjDS: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd =>
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    // 1 提取数据 2 分topic
    val orderInfoDtream: DStream[OrderInfo] = jsonObjDS.map { record =>
      val jsonStr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      orderInfo
    }

    //map-> filter -> store
    // 按照周期+分区 组成大sql查询
    // select xxx from user_state0213 where user_id in (xxx,xxx,x,xxx,xx,xx)
    val orderInfoWithFlagDstream: DStream[OrderInfo] = orderInfoDtream.mapPartitions { orderInfoItr =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList != null && orderInfoList.size > 1) {
        val userIdlist: List[Long] = orderInfoList.map { orderInfo => orderInfo.user_id }
        //select。。。in(1,2,3)
        val sql = "select  USER_ID,IF_CONSUMED from  USER_STATE0213 where USER_ID in ('" + userIdlist.mkString("','") + "')"
        val ifConsumedList: List[JSONObject] = PhoenixUtil.queryList(sql)
        //list=>List[(k,v)]=>map
        val ifConsumedMap: Map[String, String] = ifConsumedList.map { jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED")) }.toMap
        for (orderInfo <- orderInfoList) {
          val ifConsumed: String = ifConsumedMap.getOrElse(orderInfo.user_id.toString, "0")

          if (ifConsumed == "1") {
            orderInfo.if_first_order = "0" //不是首单
          } else {
            orderInfo.if_first_order = "1" //否则是首单
          }
        }
      }
      orderInfoList.iterator
    }

    //同一批次 同一个用户两次下单 如何解决 只保证第一笔订单为首单 其他订单不能为首单
    val orderInfoMap: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithFlagDstream.map(orderInfo => (orderInfo.user_id, orderInfo)).groupByKey()

    val orderInfoFlagDstream: DStream[OrderInfo] = orderInfoMap.flatMap { case (user_id, orderInfoItr) =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList != null && orderInfoList.size > 0) {
        val orderInfoAny: OrderInfo = orderInfoList(0)
        //长度大于二证明下单有多次
        if (orderInfoList.size > 2 && orderInfoAny.if_first_order == "1") {
          //对list进行排序 按createTime升序排列
          val sortedInfoList: List[OrderInfo] = orderInfoList.sortWith((orderinfo1, orderinfo2) => orderinfo1.create_time < orderinfo2.create_time)
          for (i <- 1 to sortedInfoList.size - 1) {
            //将其他orderinfo的if_first_order改成0 不是首单
            sortedInfoList(i).if_first_order = "0"
          }
          sortedInfoList
        } else {
          orderInfoList
        }
      } else {
        orderInfoList
      }
    }

    //////////////////////////
    //////关联维表 /////////
    /////////////////////
    // 查询维度表（省份)
    //map 每条数 /  mapPartition 每个分区  mapPartition针对每个分区的数据进行查询得到结果后在分发给每条数据
    //transform 把整个维度表查询出来 在分发给各个分区

    //查询维表 地区
    //driver //启动执行 只执行一次 //如果数据有可能发生变化 查询周期性查询

    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoFlagDstream.transform { rdd =>
      val sql = "select ID ,NAME,AREA_CODE ,ISO_3166_2 from GMALL0213_PROVINCE_INFO"
      //查询省份维表
      val provinceList: List[JSONObject] = PhoenixUtil.queryList(sql)
      //将省份列表转化成map                                                          id 用于关联查询
      val provinceMap: Map[lang.Long, JSONObject] = provinceList.map(jsonObj => (jsonObj.getLong("ID"), jsonObj)).toMap

      //Broadcast（广播）是相对较为常用方法功能，通常使用方式，包括共享配置文件，
      // map数据集，树形数据结构等，为能够更好更快速为TASK任务使用相关变量。
      val provinceBroadcast: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)
      //再excuter中执行
      val rddWithProvince: RDD[OrderInfo] = rdd.map { orderInfo =>
        val provinceMapFromBroadcast: Map[lang.Long, JSONObject] = provinceBroadcast.value
        val provinceObj: JSONObject = provinceMapFromBroadcast.getOrElse(orderInfo.user_id, null)
        if (provinceObj != null) {
          orderInfo.province_name = provinceObj.getString("NAME")
          orderInfo.province_area_code = provinceObj.getString("AREA_CODE")
          orderInfo.province_3166_2_code = provinceObj.getString("ISO_3166_2")
        }
        orderInfo
      }
      rddWithProvince
    }

    //写入操作
    // 1  更新  用户状态
    // 2  存储olap  用户分析    可选
    // 3  推kafka 进入下一层处理   可选
    orderInfoWithProvinceDstream.print(1000)
    orderInfoWithProvinceDstream.foreachRDD { rdd =>
      rdd.cache()
      val userStateRDD: RDD[UserState] = rdd.map(orderInfo => UserState(orderInfo.user_id.toString, "1"))
      //将数据传通过phoenix传到hbase
      userStateRDD.saveToPhoenix("USER_STATE0213",
        Seq("USER_ID", "IF_CONSUMED"),
        new Configuration,
        Some("hadoop202,hadoop203,hadoop204:2181"))

      //存储olap 用户分析
      rdd.foreachPartition { orderInfoItr =>
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        val orderInfoListMap: List[(OrderInfo, String)] = orderInfoList.map(orderInfo => (orderInfo, orderInfo.user_id.toString))
//         val dateStr=new SimpleDateFormat("yyyyMMdd").format(new Date())
//        println("============"+dateStr)
//        //如果要进行分析课发送到es
//        MyEsUtil.bulkSave(orderInfoListMap,"gmall0213_order_info_"+dateStr )

        for ((orderInfo, userId) <- orderInfoListMap) {
          //将数据发送到kafka
          MyKafkaSink.send("DWD_ORDER_INFO", userId, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
        }
      }
      //数据偏移量存到kafka
      OffsetManger.saveOffset(topic,groupId,ranges)
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
