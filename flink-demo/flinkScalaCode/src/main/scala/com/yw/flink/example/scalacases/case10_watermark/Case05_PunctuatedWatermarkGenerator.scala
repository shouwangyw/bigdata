package com.yw.flink.example.scalacases.case10_watermark

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
  * Flink 间断性生成watermark
  * 案例：读取socket基站日志数据，以基站001的事件时间为基准来触发窗口操作
  */
object PunctuatedWatermarkGeneratorTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置隐式转换
    import org.apache.flink.streaming.api.scala._

    //读取socket数据 ，格式：001,181,182,busy,1000,10
    val sourceDS: DataStream[String] = env.socketTextStream("nc_server", 9999)

    //转换成StationLog对象流
    val stationLogDS: DataStream[StationLog] = sourceDS.map(line => {
      val split: Array[String] = line.split(",")
      StationLog(split(0), split(1), split(2), split(3), split(4).toLong, split(5).toLong)
    })

    //设置自定义watermark
    val dsWithWatermark: DataStream[StationLog] = stationLogDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forGenerator[StationLog](new WatermarkGeneratorSupplier[StationLog] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
          new CustomPunctuatedWatermark();
        }
      })
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(t: StationLog, l: Long): Long = t.callTime
        })
        .withIdleness(Duration.ofSeconds(5))

    )

    //设置窗口
    dsWithWatermark.keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum("duration")
      .print()
    env.execute()
  }

}

class CustomPunctuatedWatermark extends WatermarkGenerator[StationLog] {
  //定义最大事件延迟
  val maxOutOfOrderness = 2000;

  //定义当前最大的时间时间戳
  var currentMaxTimestamp = Long.MinValue + maxOutOfOrderness + 1L;

  //每个事件调用一次
  override def onEvent(t: StationLog, l: Long, watermarkOutput: WatermarkOutput): Unit = {
    if ("001".equals(t.sid)) {
      //更新当前 currentMaxTimestamp
      currentMaxTimestamp = Math.max(currentMaxTimestamp, t.callTime)
      watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1L))

    }

  }

  //周期性每隔200ms 调用一次
  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
    //什么逻辑都不实现
  }
}