package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * Flink ReducingState 状态测试
  * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长。
  * 001,186,187,busy,1000,10
  * 002,187,186,fail,2000,20
  * 003,186,188,busy,3000,30
  * 004,187,186,busy,4000,40
  * 005,189,187,busy,5000,50
  */
object Case03_ReducingState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("nc_server", 9999)
    val ds2: DataStream[StationLog] = ds.map(line => {
      val arr: Array[String] = line.split(",")
      StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
    })

    ds2.keyBy(_.callOut)
      .process(new KeyedProcessFunction[String, StationLog, String] {

        private var reducingState: ReducingState[Long] = _

        //注册并获取状态
        override def open(parameters: Configuration): Unit = {
          //定义reducingState状态描述器注册状态，需要传入reduce聚合逻辑
          val reduceStateDescriptor = new ReducingStateDescriptor[Long]("callDuration-state", new ReduceFunction[Long] {
            override def reduce(t1: Long, t2: Long): Long = t1 + t2
          }, classOf[Long])

          //获取状态
          reducingState = getRuntimeContext.getReducingState(reduceStateDescriptor)
        }

        //来一条数据调用一次
        override def processElement(stationLog: StationLog,
                                    context: KeyedProcessFunction[String, StationLog, String]#Context,
                                    collector: Collector[String]): Unit = {
          val totalCallTime: Long = reducingState.get()
          if (totalCallTime == 0) {
            //获取当前处理时间
            val time: Long = context.timerService().currentProcessingTime()

            //注册定时器
            context.timerService().registerProcessingTimeTimer(time + 20 * 1000L)

          }

          //将当前数据的通话时长加入到状态中
          reducingState.add(stationLog.duration)
        }

        //定时器触发方法
        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext,
                             out: Collector[String]): Unit = {

          out.collect(s"主叫号码：${ctx.getCurrentKey}，近20s的通话时长为：${reducingState.get()}")

          //清空状态
          reducingState.clear()
        }
      }).print()
    env.execute()


  }

}
