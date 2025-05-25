package com.yw.flink.example.scalacases.case10_watermark

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
  * Flink 有序流中设置watermark
  * 案例:读取socket基站日志数据，每隔5s统计每个基站的通话时长
  */
object Case02_InOrderStreamEventWatermark {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置隐式转换
    import org.apache.flink.streaming.api.scala._

    //读取socket数据 ，格式：001,181,182,busy,1000,10
    val sourceDS: DataStream[String] = env.socketTextStream("node5", 9999)

    //转换成StationLog对象流
    val stationLogDS: DataStream[StationLog] = sourceDS.map(line => {
      val split: Array[String] = line.split(",")
      StationLog(split(0), split(1), split(2), split(3), split(4).toLong, split(5).toLong)
    })

    //设置watermark
    val dsWithWatermark: DataStream[StationLog] = stationLogDS.assignTimestampsAndWatermarks(
      //有序流中设置watermark
      WatermarkStrategy.forMonotonousTimestamps[StationLog]()
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          //设置事件时间，必须是毫秒
          override def extractTimestamp(stationLog: StationLog, l: Long): Long = stationLog.callTime
        })
        //设置并行度空闲时间，自动推荐watermark
        .withIdleness(Duration.ofSeconds(5))
    )

    //设置窗口，每隔5秒设置窗口
    dsWithWatermark.keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum("duration")
      .print()
    env.execute()

  }

}
