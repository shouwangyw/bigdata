package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.{lang, util}

/**
  * Flink ListState状态测试
  * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长。
  * 001,186,187,busy,1000,10
  * 002,187,186,fail,2000,20
  * 003,186,188,busy,3000,30
  * 004,187,186,busy,4000,40
  * 005,189,187,busy,5000,50
  *
  */
object Case02_ListState {
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

      private var listState: ListState[Long] = _

      //注册状态以及获取状态
      override def open(parameters: Configuration): Unit = {
        //注册状态
        val listStateDescriptor = new ListStateDescriptor[Long]("listState", classOf[Long])
        //获取状态
        listState = getRuntimeContext.getListState(listStateDescriptor)
      }

      //来一条数据处理一条数据
      override def processElement(stationLog: StationLog,
                                  context: KeyedProcessFunction[String, StationLog, String]#Context,
                                  collector: Collector[String]): Unit = {

        //获取状态结果
        val iterable: lang.Iterable[Long] = listState.get()
        if (!iterable.iterator().hasNext) {
          //说明该key 没有状态，注册20后触发的定时器
          val time: Long = context.timerService().currentProcessingTime()
          //注册定时器
          context.timerService().registerProcessingTimeTimer(time + 20 * 1000L)
        }

        //将当前数据的duration 加入到对应key的状态列表中
        listState.add(stationLog.duration)
      }

      //定时器触发调用方法
      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext,
                           out: Collector[String]): Unit = {
        //遍历对应key的liststate ,累加输出结果
        val iterable: lang.Iterable[Long] = listState.get()
        val iterator: util.Iterator[Long] = iterable.iterator()
        //定义通话总时长
        var totalCallTime: Long = 0L
        while (iterator.hasNext) {
          totalCallTime += iterator.next()
        }

        out.collect("当前主叫" + ctx.getCurrentKey + ",近20s通话时长：" + totalCallTime)

        //将状态清空
        listState.clear()
      }
    }).print()
    env.execute()
  }

}
