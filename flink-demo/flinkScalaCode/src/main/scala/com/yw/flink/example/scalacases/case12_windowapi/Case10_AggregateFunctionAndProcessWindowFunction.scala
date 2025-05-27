package com.yw.flink.example.scalacases.case12_windowapi

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - 增量和全量窗口聚合函数结合使用
  * 案例：读取socket基站日志数据，每隔5s统计每个基站平均通话时长。
  */
object Case10_AggregateFunctionAndProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val sourceDS: DataStream[String] = env.socketTextStream("node5", 9999)

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

    dsWithWatermark.keyBy(_.sid)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new AggregateFunction[StationLog, (Long, Long), Double] {
        override def createAccumulator(): (Long, Long) = (0L, 0L)

        override def add(in: StationLog, acc: (Long, Long)): (Long, Long) = (in.duration + acc._1, acc._2 + 1L)

        override def getResult(acc: (Long, Long)): Double = acc._1.toDouble / acc._2.toDouble

        override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = {
          (acc._1 + acc1._1, acc._2 + acc1._2)
        }
      }, new ProcessWindowFunction[Double, String, String, TimeWindow] {
        override def process(key: String,
                             context: Context,
                             elements: Iterable[Double],
                             out: Collector[String]): Unit = {
          //获取窗口起始时间
          val startTime: Long = context.window.getStart
          val endTime: Long = context.window.getEnd

          //获取平均通话时长
          val avgDuration: Double = elements.head
          out.collect(s"窗口范围：[$startTime~$endTime),基站ID:$key,平均通话时长：$avgDuration")
        }
      }).print()
    env.execute()


  }

}
