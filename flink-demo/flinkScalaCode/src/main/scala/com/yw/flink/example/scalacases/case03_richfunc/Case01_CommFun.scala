package com.yw.flink.example.scalacases.case03_richfunc

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.text.SimpleDateFormat

/**
 * Flink 函数接口
 */
object CommFunTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds1: DataStream[String] = env.socketTextStream("node5", 9999)
    ds1.map(new MyMapFunction()).print()
    env.execute()

  }
}
class MyMapFunction() extends  MapFunction[String,String]{
  override def map(line: String): String = {
    val split: Array[String] = line.split(",")
    val sid: String = split(0)
    val callOut: String = split(1)
    val callIn: String = split(2)
    val callType: String = split(3)
    val callTime: Long = split(4).toLong
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time: String = sdf.format(callTime)
    val duration: Long = split(5).toLong
    "基站：" + sid + "，主叫：" + callOut + "，被叫：" + callIn + "，呼叫类型：" + callType + "，呼叫时间" + time + "，呼叫时长" + duration
  }
}
