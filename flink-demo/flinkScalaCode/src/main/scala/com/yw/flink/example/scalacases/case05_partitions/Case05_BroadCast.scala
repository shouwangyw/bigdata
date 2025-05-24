package com.yw.flink.example.scalacases.case05_partitions

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * Flink 广播分区策略
  */
object Case05_BroadCast {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds1: DataStream[String] = env.socketTextStream("node5", 9999)

    val ds2: DataStream[(String, String)] = env.fromCollection(List[(String, String)](
      ("zs", "北京"),
      ("ls", "上海"),
      ("ww", "天津")
    ))

    //创建map 状态描述
    val msd = new MapStateDescriptor[String, String]("map-descriptor", classOf[String], classOf[String])
    //广播ds2
    val broadcastDS: BroadcastStream[(String, String)] = ds2.broadcast(msd)

    //关联处理流
    ds1.connect(broadcastDS).process(new BroadcastProcessFunction[String, (String, String), String] {
      //ds1流中的数据，来一条处理一条
      override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, (String, String), String]#ReadOnlyContext, collector: Collector[String]): Unit = {
        val name: String = in1.split(",")(1)
        val broadCastState: ReadOnlyBroadcastState[String, String] = readOnlyContext.getBroadcastState(msd)
        val city: String = broadCastState.get(name)
        collector.collect(in1 + "该数据对应的城市是：" + city)

      }

      //ds2流中的数据，来一条处理一次
      override def processBroadcastElement(in2: (String, String), context: BroadcastProcessFunction[String, (String, String), String]#Context, collector: Collector[String]): Unit = {
        //获取广播状态
        val broadCastState: BroadcastState[String, String] = context.getBroadcastState(msd)
        broadCastState.put(in2._1, in2._2)
      }


    }).print()

    env.execute()


  }

}
