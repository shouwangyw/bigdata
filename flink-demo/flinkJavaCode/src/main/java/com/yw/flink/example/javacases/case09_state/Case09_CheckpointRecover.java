package com.yw.flink.example.javacases.case09_state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Checkpoint状态恢复测试
 * 案例：读取socket中的数据，统计wc
 */
public class Case09_CheckpointRecover {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint
        env.enableCheckpointing(5000);
        //设置状态存储在HDFS中
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/flink-checkpoints");
        //设置checkpoint清理策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //读取socket数据实现wc
        env.socketTextStream("node5", 9999)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) -> {
                    String[] words = line.split(",");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }

                })
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) tp -> tp.f0).sum(1)
                .print();
        env.execute();
    }
}
