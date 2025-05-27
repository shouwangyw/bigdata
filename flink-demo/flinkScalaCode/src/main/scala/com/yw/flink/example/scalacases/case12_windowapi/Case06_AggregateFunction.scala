package com.yw.flink.example.scalacases.case12_windowapi

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
  * Flink : Flink Window 窗口函数 - AggregateFunction
  * 案例：读取socket基站日志数据，每隔5s统计每个基站通话总时长
  */
object Case06_AggregateFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val sourceDS: DataStream[String] = env.socketTextStream("node5", 9999)

    //转换数据
    val stationLogDS: DataStream[StationLog] = sourceDS.map(line => {
      val split: Array[String] = line.split(",")
      StationLog(split(0), split(1), split(2), split(3), split(4).toLong, split(5).toLong)
    })

    //设置watermark
    val dsWithWatermark: DataStream[StationLog] = stationLogDS.assignTimestampsAndWatermarks(
      //给乱序流设置watermark
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        //从事件中抽取事件时间，必须是毫秒
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(stationLog: StationLog, l: Long): Long = stationLog.callTime
        })
        //设置并行度空闲时间，自动推进水位线
        .withIdleness(Duration.ofSeconds(5))
    )

    //设置key ，并开窗
    dsWithWatermark.keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new AggregateFunction[StationLog, (String, Long), String] {
        //初始化累加器
        override def createAccumulator(): (String, Long) = ("", 0L)

        //累加聚合数据
        override def add(in: StationLog, acc: (String, Long)): (String, Long) = (in.sid, in.duration + acc._2)

        //返回结果
        override def getResult(acc: (String, Long)): String = s"基站：${acc._1},通话总时长：${acc._2}"

        //合并累加器
        override def merge(acc: (String, Long), acc1: (String, Long)): (String, Long) = {
          (acc._1, acc._2 + acc1._2)

        }
      }).print()
    env.execute()


  }

}
