package com.sun.gmall2020.realtime.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer

object PhoenixUtil {

  def   queryList(sql:String):List[JSONObject]={
    val configuration = new Configuration
    configuration.setBooleanIfUnset("phoenix.schema.isNamespaceMappingEnabled",true)
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[ JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181")
    val stat: Statement = conn.createStatement
    val rs: ResultSet = stat.executeQuery(sql )
    val md: ResultSetMetaData = rs.getMetaData
    while (  rs.next ) {
      val rowData = new JSONObject();
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }

}
