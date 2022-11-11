package com.yw.spark.example.streaming.cases

import java.net.URI
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
  *
  * @author yangwei
  */
object Case11_GracefullyShutdown {
  private val HDFS: String = "hdfs://node01:8020"
  private val CHECKPOINT: String = HDFS + "/checkpoint"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(CHECKPOINT, () => createSsc())

    new Thread(new MonitorStop(ssc)).start()

    ssc.start()
    ssc.awaitTermination()
  }

  def createSsc(): _root_.org.apache.spark.streaming.StreamingContext = {
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {
      Some(values.sum + status.getOrElse(0))
    }
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)

    // 设置优雅关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(CHECKPOINT)
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)
    val wordAndCount: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1))
      .updateStateByKey(updateFunc)

    wordAndCount.print()

    ssc
  }

  class MonitorStop(ssc: StreamingContext) extends Runnable {
    override def run(): Unit = {
      val fs: FileSystem = FileSystem.get(new URI(HDFS), new Configuration(), "hadoop")
      while (true) {
        try {
          TimeUnit.SECONDS.sleep(5)
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
        val state: StreamingContextState = ssc.getState()
        val bool: Boolean = fs.exists(new Path(HDFS + "/stopSpark"))

        if (bool && state == StreamingContextState.ACTIVE) {
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}
