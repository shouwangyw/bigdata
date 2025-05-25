package com.yw.flink.example.scalacases.case11_windows

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - 对KeyedStream使用会话窗口，动态设置gap间隙时间
  * 案例：读取基站日志数据，当基站为001时设置gap为3秒，其他为4s,没有通话数据时，生成窗口统计通话时长
  */
object Case03_SessionWindowWityKey2 {
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

    //指定key 并设置session 窗口
    dsWithWatermark.keyBy(_.sid)
      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[StationLog] {
        override def extract(t: StationLog): Long = {
          if ("001".equals(t.sid)) {
            3000
          } else {
            4000
          }
        }
      }))
      .process(new ProcessWindowFunction[StationLog, String, String, TimeWindow] {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[StationLog],
                             out: Collector[String]): Unit = {

          //获取窗口起始时间
          val startTime: Long = context.window.getStart
          val endTime: Long = context.window.getEnd

          //该基站ID总的通话时长
          var totalDurationTime = 0L
          for (elem <- elements) {
            totalDurationTime += elem.duration
          }
          out.collect(s"窗口范围：[$startTime~$endTime),基站ID:$key,通话总时长：$totalDurationTime")

        }
      }).print()
    env.execute()
  }

}
