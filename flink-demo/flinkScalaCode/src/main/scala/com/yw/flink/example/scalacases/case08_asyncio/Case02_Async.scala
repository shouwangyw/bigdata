package com.yw.flink.example.scalacases.case08_asyncio

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.{ExecutorService, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

/**
  * Flink Scala ：
  * 通过线程池方式来模拟异步的客户端访问外部存储系统
  */
object AsyncTest2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[Int] = env.fromCollection(List[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //异步方式查询mysql
    val result: DataStream[String] = AsyncDataStream.orderedWait(ds, new AsyncDatabaseRequest(),
      5000, TimeUnit.MILLISECONDS, 10)
    result.print()
    env.execute()


  }

}

class AsyncDatabaseRequest extends RichAsyncFunction[Int, String] {
  //准备线程池
  var executorService: ExecutorService = null

  override def open(parameters: Configuration): Unit = {
    //初始化线程池
    executorService = new ThreadPoolExecutor(10, 10,
      0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue)
  }

  //来一条数据处理一条
  override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {

    executorService.submit(new Runnable {
      override def run(): Unit = {
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false", "root", "123456")
        val pst: PreparedStatement = conn.prepareStatement("select id,name,age from async_tbl where id = ?")
        pst.setInt(1, input)
        val rst: ResultSet = pst.executeQuery()
        while (rst != null & rst.next()) {
          val id: Int = rst.getInt("id")
          val name: String = rst.getString("name")
          val age: Int = rst.getInt("age")

          //返回结果
          resultFuture.complete(List("id=" + id + ",name=" + name + ",age=" + age))
        }

        rst.close()
        pst.close()
        conn.close()
      }
    })
  }

  //异步IO执行超时如何处理
  override def timeout(input: Int, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(List("异步IO超时了！！！"))
  }

  /**
    * 关闭线程池
    */
  override def close(): Unit = {
    executorService.shutdown()
  }
}
