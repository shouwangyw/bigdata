package com.yw.flink.example.javacases.case02_transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class Case02_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.fromCollection(Arrays.asList("1,2", "3,4", "5,6", "7,8"));
        SingleOutputStreamOperator<Integer> result = ds.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String line, Collector<Integer> collector) throws Exception {
                String[] split = line.split(",");
                for (String one : split) {
                    collector.collect(Integer.valueOf(one));
                }
            }
        });
        result.print();
        env.execute();
    }
}

