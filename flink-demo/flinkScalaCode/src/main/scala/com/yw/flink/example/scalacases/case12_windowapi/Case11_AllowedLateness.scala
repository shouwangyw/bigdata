package com.yw.flink.example.scalacases.case12_windowapi

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - Window api - allowed lateness 允许数据延迟
  * 案例：读取socket 基站日志数据，每隔5秒统计每个基站的通话时长
  */
object Case11_AllowedLateness {
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

    dsWithWatermark.keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //在watermark允许延迟基础上，再延迟2s
      .allowedLateness(Time.seconds(2))
      .process(new ProcessWindowFunction[StationLog, String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {

          val start: Long = context.window.getStart
          val end: Long = context.window.getEnd

          var total = 0L
          for (elem <- elements) {
            total += elem.duration
          }
          out.collect(s"窗口范围：[$start~$end),基站ID:$key,通话总时长：$total")
        }
      }).print()
    env.execute()

  }

}
