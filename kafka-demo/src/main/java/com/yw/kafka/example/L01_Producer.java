package com.yw.kafka.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author yangwei
 */
public class L01_Producer {
    /**
     * 创建topic
     * kafka-topics.sh --create --bootstrap-server node1:9092,node2:9092,node3:9092 --topic test_topic  --partitions 2 --replication-factor 2
     */
    public static void main(String[] args) throws Exception {
        String topic = "test_topic";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        // kafka是一个可以持久化数据的MQ，数据形式是byte[]，kafka不会对数据进行干预，所以双方要约定编解码
        // kafka使用零拷贝 sendfile 系统调用实现快速的消费数据
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 现在的 producer 就是一个提供者，面向的其实是broker，虽然在使用的时候我们期望把数据写入 topic
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        /**
         * 生产数据：三种商品，每种商品有线性的3个ID，相同的商品最好去到一个分区里
         */
        while (true) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, "item" + j, "val" + i);
                    Future<RecordMetadata> sendFuture = producer.send(record);

                    RecordMetadata rm = sendFuture.get();
                    System.out.printf("key: %s, val: %s, partition: %d, offset: %d%n",
                            record.key(), record.value(), rm.partition(), rm.offset());
                }
            }
        }
    }
}
