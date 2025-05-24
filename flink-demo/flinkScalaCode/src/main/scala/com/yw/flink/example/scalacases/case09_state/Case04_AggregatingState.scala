package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink AggregatingState 聚合状态测试
  * 案例：读取基站日志数据，统计每个主叫号码通话平均时长。
  * 001,186,187,busy,1000,10
  * 002,187,186,fail,2000,20
  * 003,186,188,busy,3000,30
  * 004,187,186,busy,4000,40
  * 005,186,187,busy,5000,50
  */
object Case04_AggregatingState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    val ds2: DataStream[StationLog] = ds.map(line => {
      val arr: Array[String] = line.split(",")
      StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
    })

    ds2.keyBy(_.callOut)
      .map(new RichMapFunction[StationLog, String] {

        private var aggregatingState: AggregatingState[StationLog, Double] = _

        //注册状态，并获取状态
        override def open(parameters: Configuration): Unit = {
          //定义状态描述器，注册状态
          val aggregatingStateDescriptor = new AggregatingStateDescriptor[StationLog, Tuple2[Long, Long], Double](
            "aggregatingState",
            new AggregateFunction[StationLog, Tuple2[Long, Long], Double] {
              //创建累加器
              override def createAccumulator(): (Long, Long) = (0L, 0L)

              //添加元素
              override def add(in: StationLog, acc: (Long, Long)): (Long, Long) = (acc._1 + 1, acc._2 + in.duration)

              //获取结果
              override def getResult(acc: (Long, Long)): Double = acc._2.toDouble / acc._1.toDouble

              //聚合累加器
              override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = (acc._1 + acc1._1, acc._2 + acc1._2)
            }, classOf[Tuple2[Long, Long]]
          )

          //获取状态
          aggregatingState = getRuntimeContext.getAggregatingState(aggregatingStateDescriptor)
        }

        override def map(in: StationLog): String = {
          aggregatingState.add(in)
          s"当前主叫号码：${in.callOut},平均通话时长：${aggregatingState.get()}"
        }
      }).print()
    env.execute()
  }
}
