package com.yw.kafka.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Consumer 消费方式三：
 * 按指定时间戳自定义消费数据
 *
 * @author yangwei
 */
public class L01_Consumer03 {
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

        // kafka的consumer会动态负载均衡
        consumer.subscribe(Collections.singletonList("test_topic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("--- onPartitionsRevoked ---");
                for (TopicPartition partition : partitions) {
                    System.out.println(partition.partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("--- onPartitionsAssigned ---");
                for (TopicPartition partition : partitions) {
                    System.out.println(partition.partition());
                }
            }
        });

        /**
         * 以下代码是实际开发中，通过时间戳的方式，自定义消费数据的位置
         * 其本质核心知识是 seek 方法
         *
         * 举一反三
         * 1. 通过时间换算出offset，再通过seek自定义偏移
         * 2. 如果你自己维护offset持久化，通过seek完成
         */
        Map<TopicPartition, Long> tts = new HashMap<>();
        // 通过consumer取回自己分配的分区
        Set<TopicPartition> as = consumer.assignment();
        while (as.size() == 0) {
            consumer.poll(Duration.ofMillis(100).toMillis());
            as = consumer.assignment();
        }
        // 自己填充一个hashmap，为每个分区设置对应的时间戳
        for (TopicPartition partition : as) {
//            tts.put(partition, System.currentTimeMillis() - 10000);
            tts.put(partition, 1750416728470L);
        }
        // 通过consumer的api，取回 time index 的数据
        Map<TopicPartition, OffsetAndTimestamp> offsetTime = consumer.offsetsForTimes(tts);

        for (TopicPartition partition : as) {
            // 通过取回的offset数据，通过consumer的seek方法，修正自己的消费偏移
            OffsetAndTimestamp offsetAndTimestamp = offsetTime.get(partition);
            // 如果不是通过time换offset，而是从mysql读取回来，其本质是一样的
            long offset = offsetAndTimestamp.offset();
            System.out.println(offset);
            consumer.seek(partition, offset);
        }

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
