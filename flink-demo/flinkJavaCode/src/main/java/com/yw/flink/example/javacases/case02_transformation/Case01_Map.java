package com.yw.flink.example.javacases.case02_transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Flink map 算子测试
 */
public class Case01_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        SingleOutputStreamOperator<Integer> map = ds.map(one -> one + 1);
        map.print();

        env.execute();
    }
}
