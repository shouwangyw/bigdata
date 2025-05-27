package com.yw.flink.example.scalacases.case13_streamrelate

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink - Connect 两流合并，测试watermark
  */
object Case02_ConnectOnEventTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    //创建A流,001,181,182,busy,1000,10
    val ADS: DataStream[StationLog] = env.socketTextStream("node5", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })
    //设置watermark
    val adsWithWatermark: DataStream[StationLog] = ADS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(t: StationLog, l: Long): Long = t.callTime
        })
    )


    //创建B流,001,1000
    val BDS: DataStream[String] = env.socketTextStream("node5", 9999)
    //设置watermark
    val bdsWithWatermark: DataStream[String] = BDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = t.split(",")(1).toLong
        })
    )

    //两流进行合并
    adsWithWatermark.connect(bdsWithWatermark)
      .process(new CoProcessFunction[StationLog, String, String] {
        //处理A流
        override def processElement1(in1: StationLog,
                                     context: CoProcessFunction[StationLog, String, String]#Context,
                                     collector: Collector[String]): Unit = {
          collector.collect("A流数据：" + in1.toString + ",watermark = " + context.timerService().currentWatermark())
        }

        //处理B流
        override def processElement2(in2: String,
                                     context: CoProcessFunction[StationLog, String, String]#Context,
                                     collector: Collector[String]): Unit = {

          collector.collect("B流数据：" + in2 + ",watermark = " + context.timerService().currentWatermark())
        }
      }).print()

    env.execute()
  }

}
