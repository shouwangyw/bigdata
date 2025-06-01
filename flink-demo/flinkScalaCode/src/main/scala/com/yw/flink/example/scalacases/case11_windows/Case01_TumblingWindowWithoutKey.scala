package com.yw.flink.example.scalacases.case11_windows

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - 针对DataStream使用滚动窗口
  * 案例：读取socket基站日志数据，每隔5秒统计所有数据通话时长
  */
object Case01_TumblingWindowWithoutKey {
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

    //对所有数据设置窗口
    dsWithWatermark.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new ProcessAllWindowFunction[StationLog, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {

          //获取窗口起始时间
          val startTime: Long = context.window.getStart
          val endTime: Long = context.window.getEnd

          //统计当前基站的通话时长
          var totalDurationTime = 0L
          for (elem <- elements) {
            totalDurationTime += elem.duration
          }

          //返回数据
          out.collect(s"窗口范围：[$startTime~$endTime)，所有基站总通话时长:$totalDurationTime")


        }
      }).print()

    env.execute()
  }

}
