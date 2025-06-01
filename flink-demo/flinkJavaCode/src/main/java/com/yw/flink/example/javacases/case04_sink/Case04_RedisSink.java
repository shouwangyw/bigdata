package com.yw.flink.example.javacases.case04_sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * Flink将数据写出到Redis
 */
public class Case04_RedisSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = env.socketTextStream("nc_server", 9999)
                .flatMap((String line, Collector<String> collector) -> {
                    String[] split = line.split(",");
                    for (String word : split) {
                        collector.collect(word);
                    }

                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tp -> tp.f0)
                .sum(1);

        //连接Redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("node4")
                .setPort(6379)
                .setDatabase(1)
                .build();
        //准备RedisSink

        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<>(conf, new RedisMapper<Tuple2<String, Integer>>() {
            //指定写出到Redis什么结构、什么表
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "flink-redis-sink");
            }

            //指定Redis写出的key
            @Override
            public String getKeyFromData(Tuple2<String, Integer> tp) {
                return tp.f0;
            }

            //指定Redis写出的value
            @Override
            public String getValueFromData(Tuple2<String, Integer> tp) {
                return tp.f1 + "";
            }
        });

        result.addSink(redisSink);

        env.execute();

    }
}
