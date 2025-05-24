package com.yw.flink.example.scalacases.case05_partitions

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink Scala rescale 重缩放分区测试
  */
object Case04_Rescale {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[Object] = env.addSource(new RichParallelSourceFunction[Object] {
      override def run(ctx: SourceFunction.SourceContext[Object]): Unit = {
        val list1 = List[String]("a", "b", "c", "d", "e", "f")
        val list2 = List[Integer](1, 2, 3, 4, 5, 6)

        list1.foreach(one => {
          if (getRuntimeContext.getIndexOfThisSubtask == 0) {
            ctx.collect(one)
          }
        })
        list2.foreach(one => {
          if (getRuntimeContext.getIndexOfThisSubtask == 1) {
            ctx.collect(one)
          }
        })
      }

      override def cancel(): Unit = {
      }
    })


    ds.rescale.print().setParallelism(3)
    //    ds.rebalance.print().setParallelism(4)
    env.execute()

  }
}
