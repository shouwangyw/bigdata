package com.yw.flink.example.scalacases.case00

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 本地Webui 测试
  */
object Case04_SockerWordCountWithWebUI {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set(RestOptions.BIND_PORT, "8081")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[String] = env.socketTextStream("nc_server", 9999)
    ds.flatMap(line => {
      line.split(",")
    })
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }
}
