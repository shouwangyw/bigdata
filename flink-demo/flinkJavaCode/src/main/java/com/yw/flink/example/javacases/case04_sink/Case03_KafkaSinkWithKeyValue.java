package com.yw.flink.example.javacases.case04_sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
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
 * Flink 写出数据到Kafka ，有key,有value
 */
public class Case03_KafkaSinkWithKeyValue {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //hello,flink
        DataStreamSource<String> ds1 = env.socketTextStream("nc_server", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = ds1.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] split = s.split(",");
                        for (String word : split) {
                            collector.collect(word);
                        }
                    }
                }).map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tp -> tp.f0)
                .sum(1);


        //准备KafkaSink对象，写出key value格式数据
        KafkaSink<Tuple2<String, Integer>> kafkaSink = KafkaSink.<Tuple2<String, Integer>>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000L + "")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("flink-topic")
                                .setKeySerializationSchema(new MyKeySerializationSchema())
                                .setValueSerializationSchema(new MyValueSerializationSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        //将结果数据写出到Kafka
        result.sinkTo(kafkaSink);
        env.execute();
    }

    private static class MyKeySerializationSchema implements SerializationSchema<Tuple2<String, Integer>> {
        @Override
        public byte[] serialize(Tuple2<String, Integer> tp) {
            return tp.f0.getBytes();
        }
    }

    private static class MyValueSerializationSchema implements SerializationSchema<Tuple2<String, Integer>> {
        @Override
        public byte[] serialize(Tuple2<String, Integer> tp) {
            return tp.f1.toString().getBytes();
        }
    }
}


