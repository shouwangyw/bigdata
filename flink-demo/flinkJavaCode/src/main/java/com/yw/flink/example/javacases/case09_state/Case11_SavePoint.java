package com.yw.flink.example.javacases.case09_state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink SavePoint 保存点测试
 */
public class Case11_SavePoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("nc_server", 9999).uid("socket-source")
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] splits = s.split(",");
                    for (String word : splits) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }).uid("flatMap")
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) strIntTuple2 -> strIntTuple2.f0).sum(1).uid("sum")
                .print().uid("print");
        env.execute();
    }
}
