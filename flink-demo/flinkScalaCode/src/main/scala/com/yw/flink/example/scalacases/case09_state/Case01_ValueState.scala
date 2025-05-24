package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * Flink ValueState 状态测试
  * 案例：读取基站日志数据，统计每个主叫手机通话间隔时间，单位为毫秒
  * 001,186,187,busy,1000,10
  * 002,187,186,fail,2000,20
  * 003,186,188,busy,3000,30
  * 004,187,186,busy,4000,40
  * 005,189,187,busy,5000,50
  *
  */
object Case01_ValueState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    val ds2: DataStream[StationLog] = ds.map(line => {
      val arr: Array[String] = line.split(",")
      StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
    })

    ds2.keyBy(_.callOut).process(new KeyedProcessFunction[String, StationLog, String] {

      private var valueState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        //定义状态描述符
        val descriptor = new ValueStateDescriptor[Long]("callTimeValueState", classOf[Long])
        valueState = getRuntimeContext.getState(descriptor)
      }


      //来一条数据处理一条
      override def processElement(stationLog: StationLog,
                                  context: KeyedProcessFunction[String, StationLog, String]#Context,
                                  collector: Collector[String]): Unit = {

        //获取状态
        val callTime: Long = valueState.value()
        if (callTime == 0) {
          //当前状态中没有值，第一通话，更新状态
          valueState.update(stationLog.callTime)
        } else {
          //计算当前通话时间与状态中的时间差值
          val intervalTime: Long = stationLog.callTime - callTime
          collector.collect("当前主叫号码：" + stationLog.callOut + s",两次通话间隔：${intervalTime}")
          //更新状态
          valueState.update(stationLog.callTime)
        }

      }
    }).print()

    env.execute()
  }

}
