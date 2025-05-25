package com.yw.flink.example.javacases.case09_state;

import com.yw.flink.example.JdbcCommonUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.PreparedStatement;

/**
 * 案例：读取Kafka中数据，自己实现两阶段提交方式写出到MySQL中
 */
public class Case12_TwoPhaseCommit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint
        env.enableCheckpointing(5000);
        //设置并行度为1
//        env.setParallelism(1);

        /**
         * 读取Kafka中数据
         * 1,zs,18
         * 2,ls,19
         */
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("2pc-topic")
                .setGroupId("my-test-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //将KafkaDS写出到MySQL中
        kafkaDS.addSink(new CustomTwoPhaseCommitSinkFunction());
        env.execute();

    }

    /**
     * IN:写出数据类型
     * TXN：贯穿整个两阶段提交过程中的事务对象
     * CONTEXT：上下文
     */
    private static class CustomTwoPhaseCommitSinkFunction extends TwoPhaseCommitSinkFunction<String, JdbcCommonUtils, Void> {

        /**
         * 构建默认的构造，改法中指定TXN、CONTEXT的序列化方式
         */
        public CustomTwoPhaseCommitSinkFunction() {
            super(new KryoSerializer<>(JdbcCommonUtils.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
        }

        /**
         * 开启一个事务
         * 这里就是需要创建出来贯穿整个事务的对象
         */
        @Override
        protected JdbcCommonUtils beginTransaction() throws Exception {
            System.out.println("beginTransaction... 开启事务");
            return new JdbcCommonUtils();
        }

        /**
         * 来一条数据处理一条数据
         */
        @Override
        protected void invoke(JdbcCommonUtils jdbcCommonUtils, String value, Context context) throws Exception {
            String[] split = value.split(",");
            //获取连接
            PreparedStatement pst = jdbcCommonUtils.getConnect().prepareStatement("insert into user (id,name,age) values (?,?,?)");
            //设置pst数据
            pst.setInt(1, Integer.parseInt(split[0]));
            pst.setString(2, split[1]);
            pst.setInt(3, Integer.parseInt(split[2]));

            //执行插入操作
            pst.execute();

            //释放pst对象
            pst.close();
        }


        /**
         * 当barrier到达后执行，进行precommit,同时调用 beginTransaction 开启新的事务
         */
        @Override
        protected void preCommit(JdbcCommonUtils jdbcCommonUtils) throws Exception {
            System.out.println("barrier到达，执行preCommit ，并且开启新的事务");

        }

        /**
         * 当checkpoint完成后，调用该方法进行commit，在Flink JobManger通知完成checkpoint的方法内被调用
         */
        @Override
        protected void commit(JdbcCommonUtils jdbcCommonUtils) {
            System.out.println("commit()方法执行...");
            //提交事务
            jdbcCommonUtils.commit();

        }

        /**
         * 当代码出现异常，会调用abort方法，可以进行事务回滚
         */
        @Override
        protected void abort(JdbcCommonUtils jdbcCommonUtils) {
            System.out.println("代码出现问题，abort()方法调用");
            //回滚事务
            jdbcCommonUtils.rollback();
            //关闭对象
            jdbcCommonUtils.close();
        }
    }
}

