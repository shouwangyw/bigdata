package com.yw.flink.example.javacases.case08_asyncio;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Flink 通过线程池来模拟异步客户端连接 Mysql
 */
public class Case02_Async {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        SingleOutputStreamOperator<String> result = AsyncDataStream.unorderedWait(
                ds, new AsyncDatabaseRequest(), 5000, TimeUnit.MILLISECONDS, 10);
        result.print();
        env.execute();
    }

    private static class AsyncDatabaseRequest extends RichAsyncFunction<Integer, String> {
        //准备线程池对象
        ExecutorService executorService = null;


        //初始化资源，这里是创建线程池对象
        @Override
        public void open(Configuration parameters) throws Exception {
            /**
             * 1参数:线程池中的线程数量
             * 2参数：线程池中线程的最大数量
             * 3参数：线程池中线程的空闲时间
             * 4参数：空间时间的单位
             * 5参数：线程池中任务队列
             */
            executorService = new ThreadPoolExecutor(10, 10,
                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        }

        //每条数据调用一次
        @Override
        public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
            //通过线程池来执行sql语句
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        //创建mysql数据库连接
                        Connection conn = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false",
                                "root", "123456");
                        //准备SQL语句
                        PreparedStatement pst = conn.prepareStatement("select id,name,age from async_tbl where id = ?");

                        //设置查询参数
                        pst.setInt(1, input);
                        //执行获取结果
                        ResultSet rst = pst.executeQuery();
                        while (rst != null && rst.next()) {
                            int id = rst.getInt("id");
                            String name = rst.getString("name");
                            int age = rst.getInt("age");
                            //返回结果
                            resultFuture.complete(Collections.singletonList("id = " + id + ",name=" + name + ",age=" + age));

                        }
                        //关闭资源
                        rst.close();
                        pst.close();
                        conn.close();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        //超时方法
        @Override
        public void timeout(Integer input, ResultFuture<String> resultFuture) throws Exception {
            resultFuture.complete(Arrays.asList("异步IO方法执行超时了！！！"));
        }

        //释放资源
        @Override
        public void close() throws Exception {
            executorService.shutdown();
        }
    }
}
