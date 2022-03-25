package com.yw.flink.example

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * 自定义Source
  */
object CustomSourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val getSource: DataStream[Long] = env.addSource(new SingleParallelSource)
    val getSource: DataStream[Long] = env.addSource(new MultiParallelSource).setParallelism(2)

    val resultStream: DataStream[Long] = getSource.filter(x => x % 2 == 0)
    resultStream.setParallelism(1).print()

    env.execute()
  }
}

/**
  * 自定义单并行度source
  */
class SingleParallelSource extends SourceFunction[Long] {
  private var number = 1L
  private var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      number += 1
      sourceContext.collect(number)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

/**
  * 自定义多并行度source
  */
class MultiParallelSource extends ParallelSourceFunction[Long] {
  private var number = 1L
  private var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      number += 1
      sourceContext.collect(number)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
