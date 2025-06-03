package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * KeyBy后是简单聚合，倾斜处理
 * 思路：双重聚合（对key随机加前缀，然后聚合，去前缀再聚合），人为攒批处理
 */
public class Case09_Skew1 {
    public static void main(String[] args) throws Exception {
        //1.使用本地模式
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT, "8081");
        //使用配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //关闭算子链
        env.disableOperatorChaining();
        //设置并行度为4
        env.setParallelism(4);

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

                // 获取当前子任务的索引
                int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

                while (flag) {
                    String sid = "sid_" + random.nextInt(10);

                    // 如果是0号子任务，生成更多数据
                    if (subtaskIndex == 0) {
                        for (int i = 0; i < 100; i++) {
                            // 数据生成逻辑
                            generateData(ctx, random, sid, callTypes);
                        }
                    } else {
                        // 数据生成逻辑
                        generateData(ctx, random, sid, callTypes);
                    }
                    Thread.sleep(50);

                }

            }

            private void generateData(SourceContext<StationLog> ctx, Random random, String sid, String[] callTypes) {
                String callOut = "1811234" + (random.nextInt(9000) + 1000);
                String callIn = "1915678" + (random.nextInt(9000) + 1000);
                String callType = callTypes[random.nextInt(4)];
                Long callTime = System.currentTimeMillis();
                Long durations = Long.valueOf(random.nextInt(50) + "");
                if (sid.equals("sid_0")) {
                    for (int i = 0; i < 100; i++) {
                        ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));

                    }
                }
                ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));
            }

            //当取消对应的Flink任务时被调用
            @Override
            public void cancel() {
                flag = false;
            }
        });

        //过滤通话状态不为fail的数据
        SingleOutputStreamOperator<StationLog> ds2 = ds1.shuffle().filter((FilterFunction<StationLog>) value -> !"fail".equals(value.callType));
        SingleOutputStreamOperator<StationLog> ds3 = ds2.flatMap((FlatMapFunction<StationLog, StationLog>) (value, out) -> out.collect(value));

        //对数据随机加前缀
        SingleOutputStreamOperator<Tuple2<String, Long>> ds4 = ds3.map(new RichMapFunction<StationLog, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(StationLog value) throws Exception {
                Random random = new Random();
                return new Tuple2<>(random.nextInt(1000) + "-" + value.sid, 1L);
            }
        });

        //分组
        KeyedStream<Tuple2<String, Long>, String> ds5 = ds4.keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0);

        //聚集10000条数据进行统计一次，并将结果输出
        SingleOutputStreamOperator<Tuple2<String, Long>> ds6 = ds5.flatMap(new FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            //用于计数，满100条就输出数据
            private int count = 0;

            //创建map存储数据统计结果
            private Map<String, Long> dataMap = new HashMap<String, Long>();

            @Override
            public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
                //每条数据对count计数
                count++;
                //当前数据key
                String key = value.f0;

                if (dataMap.containsKey(key)) {
                    dataMap.put(key, dataMap.get(key) + 1L);
                } else {
                    dataMap.put(key, 1L);
                }

                if (count == 10000) {
                    //count达到10000就输出结果
                    for (Map.Entry<String, Long> one : dataMap.entrySet()) {
                        out.collect(new Tuple2<>(one.getKey(), one.getValue()));
                    }

                    //清空count，清空map
                    count = 0;
                    dataMap.clear();
                }

            }
        });

        //去掉前缀
        SingleOutputStreamOperator<Tuple2<String, Long>> ds7 = ds6
                .map((MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>) value -> {
                    String realKey = value.f0.split("-")[1];
                    return new Tuple2<>(realKey, value.f1);
                });

        //第二次keyby 并 sum聚合
        KeyedStream<Tuple2<String, Long>, String> ds8 = ds7
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = ds8.sum(1);

        result.print();

        env.execute();
    }
}
