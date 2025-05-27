package com.yw.flink.example.scalacases.case13_streamrelate

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * Flink - Window Join
 * 案例：读取订单流和支付流，进行window join
 */
object Case03_WindowJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //读取socket 订单流：订单ID，用户ID，订单金额，时间戳
    //order1,user1,10,1000
    val orderDS: DataStream[String] = env.socketTextStream("node5", 8888)
    //设置订单流的watermark
    val orderDSWithWatermark: DataStream[String] = orderDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = t.split(",")(3).toLong
        })
    )

    //读取socket 支付流：订单ID，支付金额，支付时间戳
    //order1,10,1000
    val payDS: DataStream[String] = env.socketTextStream("node5", 9999)
    //设置支付流的watermark
    val payDSWithWatermark: DataStream[String] = payDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = t.split(",")(2).toLong
        })
    )

    //对两个流进行window join
    orderDSWithWatermark.join(payDSWithWatermark)
      .where(_.split(",")(0))
      .equalTo(_.split(",")(0))
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new JoinFunction[String,String,String] {
        override def join(in1: String, in2: String): String = {
          in1 + " ===== " +in2
        }
      }).print()
    env.execute()

  }

}
