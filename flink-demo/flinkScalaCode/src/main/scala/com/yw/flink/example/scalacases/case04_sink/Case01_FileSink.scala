package com.yw.flink.example.scalacases.case04_sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.time.Duration

/**
  * Scala -Flink File Sink测试
  */
object Case01_FileSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //准备file Sink
    val fileSink: FileSink[String] = FileSink.forRowFormat(new Path(".tmp/scala-file-out"), new SimpleStringEncoder[String]("UTF-8"))
      //检查桶生成及桶内文件生成的周期，默认1分钟
      .withBucketCheckInterval(10000)
      //设置文件生成的策略
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withInactivityInterval(Duration.ofSeconds(30))
          .withMaxPartSize(MemorySize.ofMebiBytes(1024))
          .withRolloverInterval(Duration.ofSeconds(10))
          .build()
      ).build()

    //写出到文件
    ds.sinkTo(fileSink)
    env.execute()


  }

}
