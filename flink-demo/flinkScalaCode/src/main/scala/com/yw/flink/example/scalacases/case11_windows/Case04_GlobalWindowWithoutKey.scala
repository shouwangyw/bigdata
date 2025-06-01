package com.yw.flink.example.scalacases.case11_windows

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Flink - DataStream全局窗口使用
 * 案例：读取socket基站日志数据，设置全局窗口，当有3条数据时，统计通话时长
 */
object GlobalWindowWithoutKeyTest {
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

    dsWithWatermark.windowAll(GlobalWindows.create())
      .trigger(new MyGlobalCounterTrigger())
      .process(new ProcessAllWindowFunction[StationLog,String,GlobalWindow] {
        override def process(context: Context, elements: Iterable[StationLog], out: Collector[String]): Unit = {

          //统计通话时长
          var totalDurationTime = 0L
          for (elem <- elements) {
            totalDurationTime+=elem.duration
          }
          out.collect(s"全局窗口触发，最近3条数据的通话时长：$totalDurationTime")
        }
      }).print()
    env.execute()
  }

}

class MyGlobalCounterTrigger extends Trigger[StationLog,GlobalWindow]{
  //状态描述器
  private val eventCounterDescriptor = new ValueStateDescriptor[Long]("event-count", classOf[Long])
  //每来一条数据调用一次
  override def onElement(t: StationLog, l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    //获取状态
    val eventState: ValueState[Long] = triggerContext.getPartitionedState(eventCounterDescriptor)
    val count = Option(eventState.value()).getOrElse(0L)+1L
    //设置状态
    eventState.update(count)

    //判断当前key的状态是否达到3条数据，达到后就触发窗口，否则继续
    if(eventState.value() == 3L){
      //清空状态
      eventState.clear()
      TriggerResult.FIRE_AND_PURGE
    }else{
      TriggerResult.CONTINUE
    }

  }

  //当定义了基于processtime的定时器时，定时器触发会执行该方法
  override def onProcessingTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //当定义了基于Eventtime的定时器时，定时器触发会执行该方法
  override def onEventTime(l: Long, w: GlobalWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //当窗口销毁时，触发clear,一般用来清空状态
  override def clear(w: GlobalWindow, triggerContext: Trigger.TriggerContext): Unit = triggerContext.getPartitionedState(eventCounterDescriptor).clear()
}
