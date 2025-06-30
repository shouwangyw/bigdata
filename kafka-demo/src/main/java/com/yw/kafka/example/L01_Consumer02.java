package com.yw.kafka.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Consumer 消费方式二：
 * 拿到 records后，可以进一步获取到多个分区的集合，按分区进行消费
 *
 * Consumer是如何消费多分区数据？
 *
 * @author yangwei
 */
public class L01_Consumer02 {
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
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
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
                // 每次poll的时候是取多个分区的数据，且每个分区内的数据是有序的
                Set<TopicPartition> partitions = records.partitions();

                /**
                 * 如果手动提交offset
                 * 1. 按消息粒度同步提交
                 * 2. 按分区粒度同步提交
                 * 3. 按当前poll的批次同步提交
                 *
                 * 思考：如果在多个线程下
                 * 1. 以上1、3的方式不用多线程
                 * 2. 以上2的方式最容易想到多线程方式处理，有没有问题？回答：没问题
                 */
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
//                    pRecords.stream().sorted();
                    // 在一个微批里，按分区获取poll回来的数据
                    // 处理方式：1. 线性按分区处理；2. 并行按分区处理——多线程的方式
                    for (ConsumerRecord<String, String> next : pRecords) {
                        int part = next.partition();
                        long offset = next.offset();
                        System.out.printf("key: %s, val: %s, partition: %d, offset: %d%n",
                                next.key(), next.value(), part, offset);

                        // 第一种，这个是最安全的，每条记录级的更新
                        consumer.commitSync(Collections.singletonMap(
                                new TopicPartition("test_topic", part), new OffsetAndMetadata(offset)
                        ));
                    }

                    /**
                     * 因为你都分区了，拿到了分区的数据集，期望的是先对数据整体加工
                     * 小问题会出现？你怎么知道最后一条消息的offset？
                     * 感觉一定要有，kafka很傻，你拿走了多少，不关心，你告诉我你正确的最后一个小的offset
                     */
                    // 获取分区内最后一条消息的offset
                    long poff = pRecords.get(pRecords.size() - 1).offset();

                    // 这个是第二种，分区粒度提交offset
                    consumer.commitSync(Collections.singletonMap(
                            partition, new OffsetAndMetadata(poff)
                    ));
                }
                // 第三种，这个就是按poll的批次提交offset，可能会导致重复消费
                consumer.commitSync();
            }
        }
    }
}
