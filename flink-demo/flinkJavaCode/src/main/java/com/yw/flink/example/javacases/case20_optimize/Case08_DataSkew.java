package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

import java.util.Random;

/**
 * 数据倾斜
 */
public class Case08_DataSkew {
    public static void main(String[] args) throws Exception {
        //1.使用本地模式
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT, "8081");
        //使用配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.关闭算子链
        env.disableOperatorChaining();

        //3.设置并行度为4
        env.setParallelism(4);

        //4.自定义源，模拟数据源头存在数据倾斜
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

        //5.过滤通话状态不为fail的数据
        SingleOutputStreamOperator<StationLog> ds2 = ds1.shuffle().filter((FilterFunction<StationLog>) value -> !"fail".equals(value.callType));

        //6.使用flatMap对数据处理，这里仅仅是返回当前数据
        SingleOutputStreamOperator<StationLog> ds3 = ds2.flatMap((FlatMapFunction<StationLog, StationLog>) (value, out) -> out.collect(value));

        //7.对数据使用map 处理，转换成key,value格式数据，key:基站，value:计数
        SingleOutputStreamOperator<Tuple2<String, Long>> ds4 = ds3.map(new RichMapFunction<StationLog, Tuple2<String, Long>>() {


            @Override
            public Tuple2<String, Long> map(StationLog value) throws Exception {
                return new Tuple2<String, Long>(value.sid, 1L);
            }
        });

        //8.使用keyby按sid进行分组
        KeyedStream<Tuple2<String, Long>, String> ds5 = ds4.keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0);

        //9.统计每个基站对应的数据量
        SingleOutputStreamOperator<Tuple2<String, Long>> result = ds5.sum(1);

        result.print();
        env.execute();
    }
}
