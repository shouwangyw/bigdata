package com.yw.flink.example.javacases.case01_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 读取Kafka中数据 ，没有key
 */
public class Case04_KafkaSourceWithoutKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("testtopic")
                .setGroupId("my-test-group")
                //从哪个位置开始读取数据
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> result = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
        result.print();
        env.execute();
    }
}
