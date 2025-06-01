package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink MapState 映射状态测试
  * 案例：读取基站日志数据，统计主叫号码呼出的全部被叫号码及对应被叫号码通话总时长。
  * 001,186,187,busy,1000,10
  * 002,187,186,fail,2000,20
  * 003,186,188,busy,3000,30
  * 004,187,186,busy,4000,40
  * 005,186,187,busy,5000,50
  */
object Case05_MapState {
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
        private var mapState: MapState[String, Long] = _

        //注册状态及获取状态
        override def open(parameters: Configuration): Unit = {
          val mapStateDescriptor = new MapStateDescriptor[String, Long]("mapState", classOf[String], classOf[Long])
          mapState = getRuntimeContext.getMapState(mapStateDescriptor)
        }


        override def map(in: StationLog): String = {

          //获取当期被叫
          val callIn: String = in.callIn
          //获取通话时长
          val duration: Long = in.duration

          if (mapState.contains(callIn)) {
            mapState.put(callIn, mapState.get(callIn) + duration)
          } else {
            mapState.put(callIn, duration)
          }

          //组织返回的信息
          var info: String = ""
          import scala.collection.JavaConverters._
          for (key <- mapState.keys().asScala) {
            info += "被叫:" + key + s",通话时长：${mapState.get(key)} ->"
          }

          s"当前主叫：${in.callOut} ，通话信息:${info}"

        }

      }).print()
    env.execute()
  }

}
