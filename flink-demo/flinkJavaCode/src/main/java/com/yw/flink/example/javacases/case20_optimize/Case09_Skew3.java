package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Random;

/**
 * KeyBy后使用window聚合，解决数据倾斜
 */
public class Case09_Skew3 {
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

        //设置watermark
        SingleOutputStreamOperator<StationLog> ds2 = ds1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (element, recordTimestamp) -> element.callTime)
                        .withIdleness(Duration.ofSeconds(5))
        );

        //过滤通话状态不为fail的数据
        SingleOutputStreamOperator<StationLog> ds3 = ds2.shuffle().filter((FilterFunction<StationLog>) value -> !"fail".equals(value.callType));

        //转换数据
        SingleOutputStreamOperator<Tuple2<String, Long>> ds4 = ds3.map((MapFunction<StationLog, Tuple2<String, Long>>) value -> {
            Random random = new Random();
            return new Tuple2<>(random.nextInt(1000) + "-" + value.sid, value.duration);
        });

        //根据随机加前缀的key进行分组
        KeyedStream<Tuple2<String, Long>, String> ds5 = ds4.keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0);

        //设置窗口
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> ds6 =
                ds5.window(TumblingEventTimeWindows.of(Time.seconds(5)));


        //第一次聚合操作
        SingleOutputStreamOperator<Tuple2<String, Long>> ds7 =
                ds6.process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>.Context context,
                                        Iterable<Tuple2<String, Long>> iterable,
                                        Collector<Tuple2<String, Long>> collector) throws Exception {
                        Long totalDuration = 0L;
                        for (Tuple2<String, Long> info : iterable) {
                            totalDuration += info.f1;
                        }

                        //获取窗口起始时间
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                        String windowStartTime = sdf.format(context.window().getStart());
                        String windowEndTime = sdf.format(context.window().getEnd());
                        collector.collect(new Tuple2<>(key + "|" + windowStartTime + "|" + windowEndTime, totalDuration));
                    }
                });

        //对聚合结果去掉前缀，然后再次聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> result = ds7
                .map((MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>) value -> {
                    String newKey = value.f0.split("-")[1];
                    return new Tuple2<>(newKey, value.f1);
                })
                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0)
                .reduce((ReduceFunction<Tuple2<String, Long>>) (tp1, tp2) -> new Tuple2<>(tp1.f0.split("\\|")[0], tp1.f1 + tp2.f1));

        result.print();

        env.execute();
    }
}
