package com.yw.spark.example.streaming.cases

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 自定义Receiver，实现Spark Streaming接收socket数据实现单词统计
  *
  * @author yangwei
  */
object Case03_CustomReceiver {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf对象，注意这里至少给两个线程，一个线程没办法执行
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 3. 调用ReceiverStream API，将自定义的Receiver传进去
    val receiverStream = ssc.receiverStream(new CustomReceiver("node01", 9999))

    // 4. 对数据进行处理
    val result: DStream[(String, Int)] = receiverStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // 5. 打印结果
    result.print()

    // 6. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
  * 自定义Source数据源
  */
class CustomReceiver(host: String, port: Int) extends Receiver[String] (StorageLevel.MEMORY_AND_DISK_SER) with Logging {
  override def onStart(): Unit = {
    // 启动一个线程，开始接收数据
    new Thread("custom-receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  private def receive(): Unit = {
    var socket: Socket = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connected to " + host + ":" + port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      var line: String = null
      while ((line = reader.readLine()) != null && !isStopped) {
        store(line)
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }

  override def onStop(): Unit = {

  }
}
