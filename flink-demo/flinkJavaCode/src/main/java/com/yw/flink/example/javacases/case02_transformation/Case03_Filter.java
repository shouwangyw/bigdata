package com.yw.flink.example.javacases.case02_transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Filter 算子满足条件的会被留下，不满足的会被过滤
 */
public class Case03_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        SingleOutputStreamOperator<Integer> result = ds.filter((FilterFunction<Integer>) integer -> integer % 2 == 0);
        result.print();
        env.execute();
    }
}
