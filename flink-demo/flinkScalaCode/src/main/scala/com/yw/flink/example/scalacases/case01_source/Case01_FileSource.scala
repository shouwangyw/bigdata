package com.yw.flink.example.scalacases.case01_source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink Scala 读取File 文件数据
  */
object Case01_FileSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    val fileSource: FileSource[String] = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
      new Path("hdfs://mycluster/flinkdata/a.txt")).build()

    val result: DataStream[String] = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source")
    result.print()
    env.execute()
  }

}
