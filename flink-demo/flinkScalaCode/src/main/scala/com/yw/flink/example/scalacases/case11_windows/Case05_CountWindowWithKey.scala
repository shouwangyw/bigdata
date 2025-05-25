package com.yw.flink.example.scalacases.case11_windows

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - 基于keyedStream设置CountWindow使用
  * 案例：读取socket基站日志数据，每个基站每隔5条输出通话时长
  */
object Case05_CountWindowWithKey {
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
      .countWindow(5, 2)
      .process(new ProcessWindowFunction[StationLog, String, String, GlobalWindow] {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[StationLog],
                             out: Collector[String]): Unit = {
          //统计近5条通话时长
          var totalDurationTime = 0L;
          for (elem <- elements) {
            totalDurationTime += elem.duration
          }
          out.collect("当前基站:" + key + "，近5条数据通话时长：" + totalDurationTime)
        }

      }).print()
    env.execute()

  }

}
