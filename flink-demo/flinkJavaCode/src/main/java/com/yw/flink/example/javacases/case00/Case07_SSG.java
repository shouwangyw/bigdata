package com.yw.flink.example.javacases.case00;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * SlotSharingGroup 测试
 */
public class Case07_SSG {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration().set(RestOptions.BIND_PORT, "8081"));

        //设置并行度
        env.setParallelism(6);

        DataStreamSource<String> ds1 = env.socketTextStream("nc_server", 9999);

        SingleOutputStreamOperator<String> ds2 = ds1.filter(line -> line.startsWith("a"));

        SingleOutputStreamOperator<String> ds3 = ds2.flatMap((String line, Collector<String> collector) -> {
            String[] words = line.split(",");
            for (String word : words) {
                collector.collect(word);
            }
        }).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds4 = ds3
                .map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds5 = ds4
                .keyBy(tp -> tp.f0).sum(1).slotSharingGroup("xxx");

        ds5.print();

        env.execute();
    }
}
