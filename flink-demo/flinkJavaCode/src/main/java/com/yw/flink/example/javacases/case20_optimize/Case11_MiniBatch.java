package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Random;

/**
 * Flink SQL优化 - MiniBatch优化
 */
public class Case11_MiniBatch {
    public static void main(String[] args) {
        //1.使用本地模式
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT, "8081");
        //使用配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //创建流处理执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.开启minibatch
        //通过flink configuration进行参数设置
        TableConfig configuration = tableEnv.getConfig();
        //开启MiniBatch 优化，默认false
        configuration.set("table.exec.mini-batch.enabled", "true");
        //设置5秒时间处理缓冲数据,默认0s
        configuration.set("table.exec.mini-batch.allow-latency", "5 s");
        //设置每个聚合操作可以缓冲的最大记录数,默认-1，开启MiniBatch后必须设置为正值
        configuration.set("table.exec.mini-batch.size", "5000");

        DataStreamSource<StationLog> ds1 = env.addSource(new RichParallelSourceFunction<StationLog>() {
            Boolean flag = true;

            @Override
            public void run(SourceContext<StationLog> ctx) throws Exception {
                Random random = new Random();
                String[] callTypes = {"fail", "success", "busy", "barring"};
                while (flag) {
                    String sid = "sid_" + random.nextInt(10);
                    generateData(ctx, random, sid, callTypes);
                    Thread.sleep(50);

                }

            }

            private void generateData(SourceContext<StationLog> ctx, Random random, String sid, String[] callTypes) {
                String callOut = "1811234" + (random.nextInt(9000) + 1000);
                String callIn = "1915678" + (random.nextInt(9000) + 1000);
                String callType = callTypes[random.nextInt(4)];
                Long callTime = System.currentTimeMillis();
                Long durations = Long.valueOf(random.nextInt(50) + "");
                ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));
            }

            //当取消对应的Flink任务时被调用
            @Override
            public void cancel() {
                flag = false;
            }
        });

        //将DataStream 转换成 Table
        tableEnv.createTemporaryView("station_log_tbl", ds1);

        //打印表结构
        Table table = tableEnv.from("station_log_tbl");
        table.printSchema();

        TableResult result = tableEnv.executeSql("select sid,sum(duration) as totalDuration from station_log_tbl group by sid");
        result.print();
    }
}
