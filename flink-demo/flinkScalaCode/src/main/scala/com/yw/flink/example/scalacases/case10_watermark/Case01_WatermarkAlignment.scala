package com.yw.flink.example.scalacases.case10_watermark

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
  * Flink Watermark 对齐机制代码实现
  * 案例：读取socket基站数据，每个5s计算每个基站的通话总时长
  */
object Case01_WatermarkAlignment {
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
      WatermarkStrategy.forMonotonousTimestamps[StationLog]()
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(t: StationLog, l: Long): Long = t.callTime
        })
        .withIdleness(Duration.ofSeconds(5))
        //设置watermark对齐机制
        .withWatermarkAlignment("socket-source-group", Duration.ofSeconds(5), Duration.ofSeconds(2))
    )

    //设置窗口
    dsWithWatermark.keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum("duration")
      .print()
    env.execute()


  }

}
