package com.yw.flink.example.scalacases.case05_partitions

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink Forward 并行分区策略测试
  */
object Case07_Forward {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    env.setParallelism(2)
    val ds1: DataStream[Int] = env.addSource(new RichParallelSourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        for (elem <- list) {
          val index: Int = getRuntimeContext.getIndexOfThisSubtask
          if (elem % 2 == 0 && index == 0) {
            ctx.collect(elem)

          }
          if (elem % 2 != 0 && index == 1) {
            ctx.collect(elem)
          }
        }
      }

      override def cancel(): Unit = {
      }
    })

    ds1.print("ds1")
    val ds2: DataStream[String] = ds1.forward.map(one => {
      one + "xxxx"
    })
    ds2.print("ds2")
    env.execute()
  }


}
