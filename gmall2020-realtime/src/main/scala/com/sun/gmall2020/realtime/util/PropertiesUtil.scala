package com.sun.gmall2020.realtime.util

import java.io.{InputStream, InputStreamReader}
import java.util.Properties

object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(path: String) = {
    val properties: Properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(path), "UTF-8"));
    properties
  }
}
