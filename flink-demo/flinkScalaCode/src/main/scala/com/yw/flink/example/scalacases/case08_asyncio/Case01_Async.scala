package com.yw.flink.example.scalacases.case08_asyncio

import io.vertx.core.json.JsonObject
import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.{ResultSet, SQLConnection}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.asScalaBufferConverter

/**
  * Flink 异步客户端方式 实现异步IO机制
  */
object AsyncTest1 {

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

class AsyncDatabaseRequest extends RichAsyncFunction[Int, String]() {
  //定义JDBCClient对象
  var mysqlClient: JDBCClient = null

  override def open(parameters: Configuration): Unit = {
    //创建mysql连接配置
    val config: JsonObject = new JsonObject()
      .put("url", "jdbc:mysql://node2:3306/mydb?useSSL=false")
      .put("driver_class", "com.mysql.jdbc.Driver")
      .put("user", "root")
      .put("password", "123456")
    //创建VertxOptions对象
    val vo = new VertxOptions()
    //设置Vertx要使用的事件循环线程数
    vo.setEventLoopPoolSize(10)
    //设置Vertx要使用的最大工作线程数
    vo.setWorkerPoolSize(20)

    //创建Vertx对象
    val vertx: Vertx = Vertx.vertx(vo)

    //创建连接 JDBCClient 的对象
    mysqlClient = JDBCClient.createShared(vertx, config)
  }


  //来一条数据，通过异步方式查询数据库获取结果
  override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {
    mysqlClient.getConnection(new Handler[AsyncResult[SQLConnection]] {
      //获取连接，执行sql
      override def handle(sqlConnectionAsyncResult: AsyncResult[SQLConnection]): Unit = {
        if (!sqlConnectionAsyncResult.failed()) {
          //获取连接
          val connection: SQLConnection = sqlConnectionAsyncResult.result()
          //执行查询语句
          connection.query("select id,name,age from async_tbl where id = " + input,
            new Handler[AsyncResult[ResultSet]] {
              //执行sql 后的结果如何返回处理
              override def handle(resultSetAsyncResult: AsyncResult[ResultSet]): Unit = {
                if (!resultSetAsyncResult.failed()) {
                  val rst: ResultSet = resultSetAsyncResult.result()
                  rst.getRows().asScala.foreach(row => {
                    //返回结果
                    resultFuture.complete(List(row.encode()))
                  })
                }

              }
            })
        }
      }
    })
  }

  //异步IO执行超时的方法
  override def timeout(input: Int, resultFuture: ResultFuture[String]): Unit = {

    resultFuture.complete(List("数据：" + input + ",异步IO超时了！！！"))
  }

  //释放资源方法
  override def close(): Unit = {
    mysqlClient.close()
  }


}
