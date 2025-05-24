package com.yw.flink.example.javacases.case08_asyncio;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Flink 异步IO方式连接外部的MySQL 数据库
 */
public class Case01_Async {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> idDS = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        //使用异步IO机制查询MySQL中的数据
//        SingleOutputStreamOperator<String> result =
//                AsyncDataStream.unorderedWait(idDS, new AsyncDatabaseRequest(), 5000,
//                        TimeUnit.MILLISECONDS, 10);
        SingleOutputStreamOperator<String> result =
                AsyncDataStream.orderedWait(idDS, new AsyncDatabaseRequest(), 5000,
                        TimeUnit.MILLISECONDS, 10);
        result.print();
        env.execute();
    }

    private static class AsyncDatabaseRequest extends RichAsyncFunction<Integer, String> {

        JDBCClient mysqlClient = null;

        //初始化资源，
        @Override
        public void open(Configuration parameters) throws Exception {
            //创建连接mysql配置对象
            JsonObject config = new JsonObject()
                    .put("url", "jdbc:mysql://node2:3306/mydb?useSSL=false")
                    .put("driver_class", "com.mysql.jdbc.Driver")
                    .put("user", "root")
                    .put("password", "123456");

            //创建VertxOptions对象
            VertxOptions vo = new VertxOptions();
            //设置Vertx要使用的事件循环线程数
            vo.setEventLoopPoolSize(10);
            //设置Vertx要使用的最大工作线程数
            vo.setWorkerPoolSize(20);

            //创建Vertx对象
            Vertx vertx = Vertx.vertx(vo);

            //创建JDBCClient共享对象，多个Vertx 客户端可以共享一个JDBCClient对象
            mysqlClient = JDBCClient.createShared(vertx, config);
        }

        //来一条数据处理一次，第一个参数：进入的数据，第二个参数是异步IO返回结果的对象
        @Override
        public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
            //连接mysql
            mysqlClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
                @Override
                public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                    if (sqlConnectionAsyncResult.failed()) {
                        System.out.println("获取连接失败：" + sqlConnectionAsyncResult.cause().getMessage());
                        return;
                    }
                    //获取mysql连接
                    SQLConnection connetion = sqlConnectionAsyncResult.result();
                    //执行查询语句
                    connetion.query(
                            "select id,name,age from async_tbl where id = " + input,
                            new Handler<AsyncResult<ResultSet>>() {
                                //对于以上SQL 查询返回的结果如何处理
                                @Override
                                public void handle(AsyncResult<ResultSet> resultSetAsyncResult) {
                                    if (resultSetAsyncResult.failed()) {
                                        System.out.println("查询失败：" + resultSetAsyncResult.cause().getMessage());
                                        return;
                                    }

                                    //获取查询的结果
                                    ResultSet rst = resultSetAsyncResult.result();

                                    //返回结果
                                    rst.getRows().forEach(row -> {
                                        resultFuture.complete(Collections.singletonList(row.encode()));
                                    });

                                }
                            }
                    );
                }
            });
        }

        //异步IO超时处理方法
        @Override
        public void timeout(Integer input, ResultFuture<String> resultFuture) throws Exception {
            resultFuture.complete(Collections.singletonList("查询当前数据" + input + "时，异步IO超时了！！！"));
        }

        //释放资源
        @Override
        public void close() throws Exception {
            mysqlClient.close();
        }
    }
}



