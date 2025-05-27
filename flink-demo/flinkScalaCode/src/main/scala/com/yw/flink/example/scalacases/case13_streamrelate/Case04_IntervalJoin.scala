package com.yw.flink.example.scalacases.case13_streamrelate

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink - Interval Join
 * 案例：读取用户登录流和广告点击流，通过Interval Join分析用户登录前后广告点击的行文
 */
object Case04_IntervalJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    //读取登录流 ： user1,1000
    val loginDS: DataStream[String] = env.socketTextStream("node5", 8888)
    //设置loginDS的watermark
    val loginDSWithWatermark: DataStream[String] = loginDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = t.split(",")(1).toLong
        })
    )

    //读取点击流： user1,product1,1000
    val clickDS: DataStream[String] = env.socketTextStream("node5", 9999)
    //设置loginDS的watermark
    val clickDSWithWatermark: DataStream[String] = clickDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = t.split(",")(2).toLong
        })
    )

    //两流进行Interval Join
    loginDSWithWatermark.keyBy(_.split(",")(0))
      .intervalJoin(clickDSWithWatermark.keyBy(_.split(",")(0)))
      .between(Time.seconds(-2),Time.seconds(2))
      .process(new ProcessJoinFunction[String,String,String] {
        override def processElement(left: String,
                                    right: String,
                                    context: ProcessJoinFunction[String, String, String]#Context,
                                    collector: Collector[String]): Unit = {
          //获取用户ID
          val userId: String = left.split(",")(0)
          collector.collect(s"用户ID:$userId,点击了广告：$right")
        }
      }).print()
    env.execute()


  }

}
