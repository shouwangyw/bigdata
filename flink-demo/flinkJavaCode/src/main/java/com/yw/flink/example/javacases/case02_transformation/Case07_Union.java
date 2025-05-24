package com.yw.flink.example.javacases.case02_transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Case07_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));
        DataStreamSource<Integer> ds2 = env.fromCollection(Arrays.asList(6,7,8,9,10));

        //union合并
        DataStream<Integer> union = ds1.union(ds2);
        union.print();
        env.execute();
    }
}
