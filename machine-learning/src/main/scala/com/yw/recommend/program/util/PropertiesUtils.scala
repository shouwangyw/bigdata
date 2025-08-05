package com.yw.recommend.program.util

import java.util.Properties

object PropertiesUtils {
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("spark-conf.properties"))

  def getProp(name:String): String ={
     prop.getProperty(name)
  }

  def main(args: Array[String]): Unit = {
   println( PropertiesUtils.getProp("spark.streaming.app.name"))
  }
}
