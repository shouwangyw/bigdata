package com.yw.flink.example.javacases.case05_partitions;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink 广播流使用
 */
public class Case05_BroadCast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1,zs,18
        DataStreamSource<String> ds1 = env.socketTextStream("node5", 9999);
        //第二个流
        DataStreamSource<Tuple2<String, String>> ds2 = env.fromCollection(Arrays.asList(
                Tuple2.of("zs", "北京"),
                Tuple2.of("ls", "上海"),
                Tuple2.of("ww", "广州")
        ));

        //准备MapStateDesc... 对象
        MapStateDescriptor<String, String> msd = new MapStateDescriptor<>("map-descriptor", String.class, String.class);
        //广播流
        BroadcastStream<Tuple2<String, String>> broadcastDS = ds2.broadcast(msd);

        SingleOutputStreamOperator<String> result = ds1.connect(broadcastDS).process(new BroadcastProcessFunction<String, Tuple2<String, String>, String>() {
            //处理ds1 流数据，只要来一条调用一次
            @Override
            public void processElement(String s, BroadcastProcessFunction<String, Tuple2<String, String>, String>.ReadOnlyContext ctx, Collector<String> collector) throws Exception {

                String name = s.split(",")[1];
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(msd);
                String city = broadcastState.get(name);
                collector.collect(s + "本信息所在城市是：" + city);

            }

            //处理广播流中的数据，ds2,来一条处理一次
            @Override
            public void processBroadcastElement(Tuple2<String, String> tp,
                                                BroadcastProcessFunction<String, Tuple2<String, String>, String>.Context context, Collector<String> collector) throws Exception {
                BroadcastState<String, String> broadcastState = context.getBroadcastState(msd);
                broadcastState.put(tp.f0, tp.f1);
            }
        });

        result.print();
        env.execute();

    }

}
