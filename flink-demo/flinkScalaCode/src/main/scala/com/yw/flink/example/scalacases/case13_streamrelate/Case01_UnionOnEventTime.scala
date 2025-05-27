package com.yw.flink.example.scalacases.case13_streamrelate

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
 * Flink 基于事件时间下的union流关联
 * 案例：读取socket中数据流形成两个流，进行Union关联后设置窗口，每隔5秒统计每个基站通话次数。
 */
object Case01_UnionOnEventTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    //创建A流,001,181,182,busy,1000,10
    val ADS: DataStream[StationLog] = env.socketTextStream("node5", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })
    //设置watermark
    val adsWithWatermark: DataStream[StationLog] = ADS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(t: StationLog, l: Long): Long = t.callTime
        })
    )


    //创建B流,001,181,182,busy,1000,10
    val BDS: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })
    //设置watermark
    val bdsWithWatermark: DataStream[StationLog] = BDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(t: StationLog, l: Long): Long = t.callTime
        })
    )

    //两流进行合并
    adsWithWatermark.union(bdsWithWatermark)
      .keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new ProcessWindowFunction[StationLog,String,String,TimeWindow] {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[StationLog],
                             out: Collector[String]): Unit = {

          println("window - watermark值："+context.currentWatermark)
          //获取窗口起始时间
          val startTime: Long = context.window.getStart
          val endTime: Long = context.window.getEnd
          //获取通话次数
          val sum: Int = elements.size
          //输出结果
          out.collect(s"窗口范围：[$startTime~$endTime),基站ID:$key,通话次数：$sum")


        }
      }).print()
    env.execute()
  }

}
