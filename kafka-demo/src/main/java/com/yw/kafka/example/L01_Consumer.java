package com.yw.kafka.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author yangwei
 *
 * Consumer 消费方式一：
 * 拿到 records 消息集合，一条一条的处理
 */
public class L01_Consumer {
    public static void main(String[] args) {
        // 基础配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 消费的细节
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ooxx");
        // kafka是一个MQ，也是一个存储
        // 第一次启动，没有offset
        /**
         * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server
         * (e.g. because that data has been deleted):
         * <ul>
         *     <li>earliest: automatically reset the offset to the earliest offset
         *     <li>latest: automatically reset the offset to the latest offset</li>
         *     <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>
         *     <li>anything else: throw exception to the consumer.</li>
         * </ul>
         */
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 自动提交时异步提交，丢数据&&重复数据
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 一个运行的consumer，那么自己会维护自己的消费进度，一旦你自动提交，但是是异步的
        // 1. 还没到时间，挂了，没提交，重启一个consumer，参照offset的时候，会重复消费
        // 2. 一个批次的数据还没写入数据库成功，但是这个批次的offset被异步提交了，挂了，重启一个consumer，参照offset的时候，会丢失数据
//        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "15000");
        // poll拉取数据，弹性按需，拉取多少？
//        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("test_topic"));

        while (true) {
            /**
             * 常识：如果想多线程处理多分区，每poll一次，用一个语义：一个job启动
             * 一次job用多线程并行处理分区，且job应该被控制是串行的
             */
            // 0~n 条，微批的感觉
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0).toMillis());

            if (!records.isEmpty()) {
                // 以下代码的优化很重要
                System.out.println("---------" + records.count() + "---------");

                for (ConsumerRecord<String, String> record : records) {
                    // 因为一个consumer可以消费多个分区，但是一个分区只能给一个组里的一个consumer消费
                    System.out.printf("key: %s, val: %s, partition: %d, offset: %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }
    }
}
