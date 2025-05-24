package com.yw.flink.example.javacases.case05_partitions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * F|link 自定义分区策略
 * a,1
 * b,1
 * c,1
 * ...
 */
public class Case08_CustomPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] split = line.split(",");

                return new Tuple2<>(split[0], Integer.valueOf(split[1]));
            }
        });

        //自定义分区策略
        DataStream<Tuple2<String, Integer>> result = ds2.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                return key.hashCode() % numPartitions;
            }
        }, new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        result.print();

        env.execute();
    }
    
}
