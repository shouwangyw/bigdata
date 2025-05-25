package com.yw.flink.example.scalacases.case10_watermark

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
  * Flink 自定义watermark生成，设置事件最大延迟时间时2000ms
  * 实现的就是forBoundedOutOfOrderness
  */
object PeriodicWatermarkGeneratorTest {
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

    //设置自定义watermark
    val dsWithWatermark: DataStream[StationLog] = stationLogDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forGenerator[StationLog](new WatermarkGeneratorSupplier[StationLog] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[StationLog] = {
          new CustomPeriodicWatermark();
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

class CustomPeriodicWatermark extends WatermarkGenerator[StationLog] {
  //定义最大事件延迟
  val maxOutOfOrderness = 2000;

  //定义当前最大的时间时间戳
  var currentMaxTimestamp = Long.MinValue + maxOutOfOrderness + 1L;

  //每个事件调用一次
  override def onEvent(t: StationLog, l: Long, watermarkOutput: WatermarkOutput): Unit = {
    //更新当前 currentMaxTimestamp
    currentMaxTimestamp = Math.max(currentMaxTimestamp, t.callTime)
  }

  //周期性每隔200ms 调用一次
  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
    //周期性的发送watermark
    watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1L))
  }
}
