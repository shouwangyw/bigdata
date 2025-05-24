package com.yw.flink.example.javacases.case05_partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FlinK Rebalance 轮询分区
 */
public class Case03_Rebalance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
//        ds.rebalance().print().setParallelism(3);
        ds.print().setParallelism(3);
        env.execute();
    }
}
