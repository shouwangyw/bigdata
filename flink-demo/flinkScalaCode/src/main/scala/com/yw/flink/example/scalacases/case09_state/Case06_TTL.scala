package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink 状态生存时间TTL 测试
  * 案例:读取Socket中基站通话数据，统计每个主叫通话总时长。
  *
  */
object Case06_TTL {
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
      .map(new RichMapFunction[StationLog, String] {
        private var valueState: ValueState[Long] = _

        //注册状态并获取状态
        override def open(parameters: Configuration): Unit = {
          //创建TTL 配置
          val ttlConfig: StateTtlConfig = StateTtlConfig
            .newBuilder(Time.seconds(10))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()

          //注册状态
          val valueStateDescriptor = new ValueStateDescriptor[Long]("valueState", classOf[Long])

          //应用ttl
          valueStateDescriptor.enableTimeToLive(ttlConfig)

          //获取状态
          valueState = getRuntimeContext.getState(valueStateDescriptor)
        }

        override def map(in: StationLog): String = {
          //获取状态
          val stateValue: Long = valueState.value()
          if (stateValue == 0) {
            valueState.update(in.duration)
          } else {
            valueState.update(in.duration + stateValue)
          }

          s"当前主叫：${in.callOut},通话总时长:${valueState.value()}"

        }
      }).print()
    env.execute()
  }

}
