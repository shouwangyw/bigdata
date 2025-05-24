package com.yw.flink.example.scalacases.case01_source

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

/**
  * Scala - Flink自定义Source - 单并行度
  */
object Case05_NoParallelSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val sourceDS: DataStream[StationLog] = env.addSource(new SourceFunction[StationLog] {
      var flag = true

      //循环产生数据
      override def run(ctx: SourceFunction.SourceContext[StationLog]): Unit = {
        val random = new Random()
        val callTypes = Array[String]("fail", "success", "busy", "barring")

        while (flag) {
          val sid = "sid_" + random.nextInt(10)
          val callOut = "1811234" + (random.nextInt(9000) + 1000)
          val callIn = "1825678" + (random.nextInt(9000) + 1000)
          val callType = callTypes(random.nextInt(4))
          val callTime = System.currentTimeMillis()
          val duration = random.nextInt(50).toLong

          ctx.collect(StationLog(sid, callOut, callIn, callType, callTime, duration))
          Thread.sleep(2000)
        }

      }

      //当Flink应用程序取消时，触发执行
      override def cancel(): Unit = {
        flag = false

      }
    })

    sourceDS.print()
    env.execute()
  }
}
