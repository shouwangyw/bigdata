package com.yw.flink.example.javacases.case01_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * Java Flink 读取Kafka 数据 - 读取key 读取Value
 * kafka-console-producer.sh --bootstrap-server node1:9092,node2:9092,node3:9092 --topic testtopic --property parse.key=true --property key.separator='-'
 */
public class Case03_KafkaSourceWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Tuple2<String, String>> kafkaSource = KafkaSource.<Tuple2<String, String>>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("testtopic").setGroupId("key-value-g")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaRecordDeserializationSchema<Tuple2<String, String>>() {
                    //读取kafka数据，如何获取key和value
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Tuple2<String, String>> collector) throws IOException {
                        String key = null;
                        String value = null;
                        if (consumerRecord.key() != null) {
                            key = new String(consumerRecord.key(), "UTF-8");
                        }
                        if (consumerRecord.value() != null) {
                            value = new String(consumerRecord.value(), "UTF-8");
                        }

                        collector.collect(Tuple2.of(key, value));

                    }

                    //指定collector 发送数据的类型
                    @Override
                    public TypeInformation<Tuple2<String, String>> getProducedType() {
                        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                        });
                    }
                }).build();

        //读取Kafka中数据
        DataStreamSource<Tuple2<String, String>> result = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-key-value");
        result.print();
        env.execute();
    }
}
