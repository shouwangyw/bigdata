package com.yw.flink.example.scalacases.case11_windows

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - 针对keyedStream设置滑动窗口
  * 案例：读取socket基站日志数据，每隔5秒统计最近10秒基站通话时长
  */
object Case02_SlidingWindowWithKey {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val sourceDS: DataStream[String] = env.socketTextStream("nc_server", 9999)

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

    //设置key,设置滑动窗口
    dsWithWatermark.keyBy(_.sid)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .process(new ProcessWindowFunction[StationLog, String, String, TimeWindow] {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[StationLog],
                             out: Collector[String]): Unit = {

          //获取窗口的起始时间
          val startTime: Long = if (context.window.getStart < 0) 0 else context.window.getStart
          val endTime: Long = context.window.getEnd

          //该基站对应的通话总时长
          var totalDurationTime = 0L;

          for (elem <- elements) {
            totalDurationTime += elem.duration
          }
          out.collect(s"当前窗口:[$startTime~$endTime),基站ID:$key,通话总时长:$totalDurationTime")


        }
      }).print()
    env.execute()
  }

}
