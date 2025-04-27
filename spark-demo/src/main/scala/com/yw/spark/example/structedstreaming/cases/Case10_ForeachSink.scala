package com.yw.spark.example.structedstreaming.cases

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

/**
  * 读取scoket数据写入memory内存，再读取
  */
object Case10_ForeachSink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()

    val personDF: DataFrame = df.as[String].map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0).toInt, arr(1), arr(2).toInt)
    }).toDF("id", "name", "age")

    personDF.writeStream
      .foreach(new ForeachWriter[Row]() {
        var conn: Connection = _
        var pst: PreparedStatement = _

        // 打开资源
        override def open(partitionId: Long, epochId: Long): Boolean = {
          conn = DriverManager.getConnection("jdbc:mysql://node2:3306/testdb", "root", "123456")
          pst = conn.prepareStatement("insert into person values (?,?,?)")
          true
        }

        // 一条条处理数据
        override def process(row: Row): Unit = {
          val id: Int = row.getInt(0)
          val name: String = row.getString(1)
          val age: Int = row.getInt(2)
          pst.setInt(1, id)
          pst.setString(2, name)
          pst.setInt(3, age)
          pst.executeUpdate()
        }

        // 关闭释放资源
        override def close(errorOrNull: Throwable): Unit = {
          pst.close()
          conn.close()
        }
      }).start()
      .awaitTermination()
  }
}