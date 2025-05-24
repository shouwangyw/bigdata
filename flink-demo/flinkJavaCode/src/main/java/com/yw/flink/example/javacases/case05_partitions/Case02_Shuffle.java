package com.yw.flink.example.javacases.case05_partitions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 随机分区
 *  上游数据随机打散方式发送到下游
 */
public class Case02_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        ds.shuffle().print().setParallelism(3);
        env.execute();

    }
}
