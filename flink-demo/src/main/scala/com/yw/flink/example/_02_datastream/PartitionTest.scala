package com.yw.flink.example._02_datastream

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 测试重新分区：对filter之后的数据进行重新分区
  */
object PartitionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.fromCollection(1 to 100)

    val filterStream = dataStream.filter(x => x > 10)
//      .shuffle // 随机的重新分发数据：上游的数据，随机的发送到下游的分区里面去
      .rebalance // 对数据重新进行分区，涉及到shuffle的过程
//      .rescale // 跟rebalance有点类似，但不是全局的，这种方式仅发生在一个单一的节点，因此没有跨网络的数据传输。

    // 带有Rich的类，表示富函数类，它的功能比较强大，在内部是可以获取state、分布式缓存、广播变量、运行时的上下文对象等
    val resultStream = filterStream.map(new RichMapFunction[Int, (Int, Int)] {
      override def map(value: Int): (Int, Int) = {
        // 获取任务id，以及value
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    })

    resultStream.print()

    env.execute()
  }
}
