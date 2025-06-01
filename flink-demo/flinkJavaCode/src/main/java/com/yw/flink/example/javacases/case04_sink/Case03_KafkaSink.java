package com.yw.flink.example.javacases.case04_sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 读取Socket数据统计wordCount 写入到Kafka中
 */
public class Case03_KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //hello,flink
        DataStreamSource<String> ds1 = env.socketTextStream("nc_server", 9999);
        SingleOutputStreamOperator<String> result = ds1.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] split = s.split(",");
                        for (String word : split) {
                            collector.collect(word);
                        }
                    }
                }).map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tp -> tp.f0)
                .sum(1)
                .map(tp -> tp.f0 + "-" + tp.f1);

        //准备KafkaSink对象
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000L + "")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("flink-topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        //将结果数据写出到Kafka
        result.sinkTo(kafkaSink);
        env.execute();

    }
}
