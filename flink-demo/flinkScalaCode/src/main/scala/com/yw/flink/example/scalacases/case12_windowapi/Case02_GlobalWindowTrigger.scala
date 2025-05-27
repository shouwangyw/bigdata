package com.yw.flink.example.scalacases.case12_windowapi

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink - 基于GlobalWindow 设置自定义触发器
 * 案例:读取基站日志数据，设置globalwindow，手动指定触发器，每个基站每5秒生成窗口。
 */
object GlobalWindowTriggerTest {
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

    //设置key，并设置全局窗口，通过自定义触发器方式触发
    dsWithWatermark.keyBy(_.sid)
      .window(GlobalWindows.create())
      .trigger(new MyTimeTrigger2())
      .process(new ProcessWindowFunction[StationLog,String,String,GlobalWindow] {
        override def process(key: String, context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit ={
          //统计通话时长
          var totalDurationTime = 0L;
          for (elem <- elements) {
            totalDurationTime+=elem.duration
          }
          out.collect(s"基站:$key，通话总时长：$totalDurationTime")

        }
      }).print()
    env.execute()
  }

}

class MyTimeTrigger2 extends Trigger[StationLog,GlobalWindow]{
  //设置状态
  private val timerStateDescriptor = new ValueStateDescriptor[Boolean]("timer-state", classOf[Boolean])

  override def onElement(stationLog: StationLog, timestamp: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {

    //获取状态
    val isExist: Boolean = triggerContext.getPartitionedState(timerStateDescriptor).value()
    if(isExist ==null || !isExist){
      //设置定时器
      triggerContext.registerEventTimeTimer(timestamp+4999L)

      //更新状态
      triggerContext.getPartitionedState(timerStateDescriptor).update(true)
    }
    TriggerResult.CONTINUE

  }

  override def onProcessingTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    //..
    TriggerResult.CONTINUE

  }

  override def onEventTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    //设置状态
    triggerContext.getPartitionedState(timerStateDescriptor).update(false)
    TriggerResult.FIRE_AND_PURGE

  }

  override def clear(w: GlobalWindow, triggerContext: Trigger.TriggerContext): Unit = {
    //...

  }
}
