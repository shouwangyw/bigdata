package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case04_KeyBy {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("c", 5)))
    ds.keyBy(one => {
      one._1
    }).sum(1).print()
    env.execute()

  }

}
