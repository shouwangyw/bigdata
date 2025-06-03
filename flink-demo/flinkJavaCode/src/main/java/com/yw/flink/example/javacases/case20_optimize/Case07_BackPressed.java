package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * 反压测试
 */
public class Case07_BackPressed {
    public static void main(String[] args) throws Exception {
        //1.使用本地模式
        Configuration conf = new Configuration();
        conf.setString("rest.flamegraph.enabled", "true");
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT, "8081");
        //使用配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.disableOperatorChaining();
        env.setParallelism(8);

        DataStreamSource<StationLog> ds1 = env.addSource(new RichParallelSourceFunction<StationLog>() {
            Boolean flag = true;

            /**
             * 主要方法:启动一个Source，大部分情况下都需要在run方法中实现一个循环产生数据
             * 这里计划1s 产生1条基站数据，由于是并行，当前节点有几个core就会有几条数据
             */
            @Override
            public void run(SourceContext<StationLog> ctx) throws Exception {
                Random random = new Random();
                String[] callTypes = {"fail", "success", "busy", "barring"};
                while (flag) {
                    String sid = "sid_" + random.nextInt(10);

                    if (sid.equals("sid_0")) {
                        for (int i = 0; i < 1000; i++) {
                            String callOut = "1811234" + (random.nextInt(9000) + 1000);
                            String callIn = "1915678" + (random.nextInt(9000) + 1000);
                            String callType = callTypes[random.nextInt(4)];
                            Long callTime = System.currentTimeMillis();
                            Long durations = Long.valueOf(random.nextInt(50) + "");

                            ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));

                        }
                        Thread.sleep(200);

                    } else {
                        String callOut = "1811234" + (random.nextInt(9000) + 1000);
                        String callIn = "1915678" + (random.nextInt(9000) + 1000);
                        String callType = callTypes[random.nextInt(4)];
                        Long callTime = System.currentTimeMillis();
                        Long durations = Long.valueOf(random.nextInt(50) + "");

                        ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));
                        Thread.sleep(200);
                    }

                }

            }

            //当取消对应的Flink任务时被调用
            @Override
            public void cancel() {
                flag = false;
            }
        });

        //过滤通话状态为fail的数据
        SingleOutputStreamOperator<StationLog> ds2 = ds1.filter((FilterFunction<StationLog>) value -> !"fail".equals(value.callType));

        //对数据进行keyby
        KeyedStream<StationLog, String> ds3 = ds2.keyBy((KeySelector<StationLog, String>) value -> value.sid);

        //对数据进行聚合操作
        SingleOutputStreamOperator<StationLog> ds4 = ds3.sum("duration");

        SingleOutputStreamOperator<String> result = ds4.map((MapFunction<StationLog, String>) value -> {
//                Thread.sleep(1);
            return "基站ID：" + value.sid + ",通话时长：" + value.duration;
        }).setParallelism(9);

        result.print();

        env.execute();
    }
}
