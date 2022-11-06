package com.yw.spark.example.streaming.cases

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 将WordCount案例中得到的结果通过foreachRDD保存结果到mysql中
  *
  * @author yangwei
  */
object Case08_WordCountForeachRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 1. 创建SparkConf对象，注意这里至少给两个线程，一个线程没办法执行
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 3. 接收Socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    // 4. 对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // 5. 将结果保存到MySQL数据库中
//    /*********************** 方案一 ***********************/
//    result.foreachRDD(rdd => {
//      // 注意这里创建的对象都是在Driver端，但真正执行是在 Executor 端，所以是有问题的
//      val conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "root", "123456")
//      val statement = conn.prepareStatement(s"insert into wordcount(word, count) values(?, ?)")
//
//      rdd.foreach { record =>
//        statement.setString(1, record._1)
//        statement.setInt(2, record._2)
//        statement.execute()
//      }
//      statement.close()
//      conn.close()
//    })

//    /*********************** 方案二 ***********************/
//    result.foreachRDD(rdd => {
//      rdd.foreach { record =>
//        // 针对每一个record创建连接，效率不高
//        val conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "root", "123456")
//        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values(?, ?)")
//
//        statement.setString(1, record._1)
//        statement.setInt(2, record._2)
//        statement.execute()
//
//        statement.close()
//        conn.close()
//      }
//    })

//    /*********************** 方案三 ***********************/
//    result.foreachRDD(rdd => {
//      rdd.foreachPartition(it => {
//        // 针对每一个执行器分区创建连接
//        val conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "root", "123456")
//        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values(?, ?)")
//
//        it.foreach(record => {
//          statement.setString(1, record._1)
//          statement.setInt(2, record._2)
//          statement.execute()
//        })
//
//        statement.close()
//        conn.close()
//      })
//    })

    /*********************** 方案四 ***********************/
    result.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        // 针对每一个执行器分区创建连接，同时使用批量提交
        val conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values(?, ?)")

        // 关闭自动提交
        conn.setAutoCommit(false)
        it.foreach(record => {
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          // 添加到每一个批次
          statement.addBatch()
        })
        // 批量提交该分区所有数据
        statement.executeBatch()
        conn.commit()

        statement.close()
        conn.close()
      })
    })

    // 6. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
