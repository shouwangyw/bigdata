package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.{PersonInfo, StationLog}
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * Flink 广播状态 测试
 * 案例：读取两个socket数据，8888，9999端口
 * A流为基站日志数据，B流为通话的人的基本信息
 * B流广播与A流进行关联，输出通话的详细信息
 *
 */
object Case08_BroadCast {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //主流
    val mainDS: DataStream[StationLog] = env.socketTextStream("node5", 8888)
      .map(line => {
        val split: Array[String] = line.split(",")
        StationLog(split(0), split(1), split(2), split(3), split(4).toLong, split(5).toLong)
      })

    //mapstate 状态描述器
    val mapStateDescriptor = new MapStateDescriptor[String, PersonInfo]("mapState", classOf[String], classOf[PersonInfo])
    //广播流
    val broadCastStream: BroadcastStream[PersonInfo] = env.socketTextStream("node5", 9999)
      .map(line => {
        val split: Array[String] = line.split(",")
        PersonInfo(split(0), split(1), split(2))
      }).broadcast(mapStateDescriptor)

    //两流关联
    mainDS.connect(broadCastStream)
      .process(new BroadcastProcessFunction[StationLog,PersonInfo,String] {
        //主流中的数据处理
        override def processElement(stationLog: StationLog,
                                    readOnlyContext: BroadcastProcessFunction[StationLog, PersonInfo, String]#ReadOnlyContext,
                                    collector: Collector[String]): Unit = {
          val broadCastState: ReadOnlyBroadcastState[String, PersonInfo] = readOnlyContext.getBroadcastState(mapStateDescriptor)
          val callOutPersonInfo: PersonInfo = broadCastState.get(stationLog.callOut)
          val callInPersonInfo: PersonInfo = broadCastState.get(stationLog.callIn)
          val callOutName: String = Option(callOutPersonInfo).map(_.name).getOrElse("无姓名")
          val callOutCity: String = Option(callOutPersonInfo).map(_.city).getOrElse("无城市")
          val callInName: String = Option(callInPersonInfo).map(_.name).getOrElse("无姓名")
          val callInCity: String = Option(callInPersonInfo).map(_.city).getOrElse("无城市")

          collector.collect(s"主叫：${callOutName},主叫城市:${callOutCity},被叫：${callInName},被叫城市:${callInCity},通话时长：${stationLog.duration}")

        }

        //广播流中的数据处理
        override def processBroadcastElement(personInfo: PersonInfo,
                                             context: BroadcastProcessFunction[StationLog, PersonInfo, String]#Context,
                                             collector: Collector[String]): Unit = {

            context.getBroadcastState(mapStateDescriptor).put(personInfo.phoneNum,personInfo)
        }
      }).print()
    env.execute()
  }

}
