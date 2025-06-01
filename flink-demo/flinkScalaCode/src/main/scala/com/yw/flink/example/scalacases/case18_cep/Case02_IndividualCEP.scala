package com.yw.flink.example.scalacases.case18_cep

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util

/**
  * 简单事件定义模式 ： 设置量词和条件
  *
  */
object Case02_IndividualCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //1.定义事件流
    val ds: DataStream[String] = env.socketTextStream("nc_server", 9999)

    //2.定义匹配规则
    val pattern: Pattern[String, String] = Pattern.begin[String]("first").where(_.startsWith("a"))
      .oneOrMore
      .until(_.equals("end"))

    //3.将规则应用到事件流上
    val patternStream: PatternStream[String] = CEP.pattern(ds, pattern).inProcessingTime()

    //4.获取匹配结果
    val result: DataStream[String] = patternStream.select(new PatternSelectFunction[String, String] {
      override def select(map: util.Map[String, util.List[String]]): String = {
        val list: util.List[String] = map.get("first")
        import scala.collection.JavaConverters._
        list.asScala.mkString("-")
      }
    })

    result.print()
    env.execute()
  }

}
