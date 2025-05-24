package com.yw.flink.example.scalacases.case06_sideoutput

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * Flink侧输出流测试
 */
object SideOutput {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)

    //定义侧输出流的标签
    val outputTag = new OutputTag[String]("side-output")

    val mainDS: DataStream[String] = ds.process(new ProcessFunction[String, String] {
      override def processElement(line: String, ctx: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
        //001,186,187,success,1000,10
        if ("success".equals(line.split(",")(3))) {
          //通话成功数据
          collector.collect(line)
        } else {
          ctx.output(outputTag, line)
        }
      }
    })

    //打印主流
    mainDS.print("主流")
    //打印侧流
    mainDS.getSideOutput(outputTag).print("侧流")
    env.execute()

  }


}
