package com.yw.flink.example.scalacases.case07_processfun

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 案例：Flink读取Socket中通话数据，如果被叫手机连续5s呼叫失败生成告警信息。
 */
object ProcessFun {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    val ds2: DataStream[StationLog] = ds.map(one => {
      val split: Array[String] = one.split(",")
      StationLog(split(0), split(1), split(2), split(3), split(4).toLong, split(5).toLong)
    })

    //按照被叫手机号分组
    val keyedStream: KeyedStream[StationLog, String] = ds2.keyBy(stationLog => {
      stationLog.callIn
    })

    //使用processFunction 进行定时器注册和触发
    keyedStream.process(new KeyedProcessFunction[String,StationLog,String] {
      //定义一个状态 ，记录当前被叫手机号定时器触发时间
      lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

      //来一条数据处理一次
      override def processElement(stationLog: StationLog,
                                  ctx: KeyedProcessFunction[String, StationLog, String]#Context,
                                  collector: Collector[String]): Unit = {
        //获取当前被叫手机号的定时器触发时间
        val time: Long = timeState.value()

        //如果来的这条通话数据通话状态为 fail ,并且状态中没有值，注册定时器
        if("fail".equals(stationLog.callType) && time == 0){
          //获取当前时间
          val nowTime: Long = ctx.timerService().currentProcessingTime()
          //定时器触发时间
          val triggerTime = nowTime + 5000L
          //注册定时器
          ctx.timerService().registerProcessingTimeTimer(triggerTime)
          //更新状态
          timeState.update(triggerTime)
        }

        if(!"fail".equals(stationLog.callType) && time != 0){
          //删除定时器
          ctx.timerService().deleteProcessingTimeTimer(time)

          //清空状态
          timeState.clear()
        }


      }

      /**
       * 定时器触发方法
       */
      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext,
                           out: Collector[String]): Unit ={
        out.collect("当前触发时间："+timestamp+",手机号："+ctx.getCurrentKey+"连续5秒呼叫失败！！！")
        //清空状态
        timeState.clear()
      }

    }).print()

    env.execute()

  }

}
