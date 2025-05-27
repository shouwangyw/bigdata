package com.yw.flink.example.scalacases.case12_windowapi

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - window api - evictor 使用
  * 案例：读取socket中基站日志数据，每隔5秒统计每个基站的通话时长
  * 要求：如果一个窗口中的数据与该窗口最后一条数据通话时长相差5秒，就剔除数据
  */
object Case03_DeltaEvictor {
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

    //指定基站为key,设置窗口
    dsWithWatermark.keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .evictor(DeltaEvictor.of[StationLog, TimeWindow](5, new DeltaFunction[StationLog] {
        override def getDelta(data: StationLog, data1: StationLog): Double = Math.abs(data.duration - data1.duration)
      }))
      .process(new ProcessWindowFunction[StationLog, String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {
          //获取窗口起始时间
          val startTime: Long = context.window.getStart
          val endTime: Long = context.window.getEnd

          //统计通话总时长
          var totalDurationTime = 0L;
          for (elem <- elements) {
            totalDurationTime += elem.duration
          }
          out.collect(s"窗口范围：[$startTime~$endTime),基站ID:$key,通话总时长:$totalDurationTime")

        }
      }).print()
    env.execute()

  }

}
