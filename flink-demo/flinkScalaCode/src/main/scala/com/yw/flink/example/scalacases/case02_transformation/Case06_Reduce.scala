package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case06_Reduce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 1), ("c", 1), ("c", 1), ("b", 3)))
    val result: DataStream[(String, Int)] = ds.keyBy(_._1).reduce((t1, t2) => {
      (t1._1, t1._2 + t2._2)
    })
    result.print()
    env.execute()

  }

}
