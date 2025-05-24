//package com.yw.flink.example.javacases.case04_sink;
//
//import com.yw.flink.example.StationLog;
//import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.util.function.SerializableSupplier;
//
//import javax.sql.XADataSource;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//
///**
// * Flink 写出JDBC exactly-once 实现
// */
//public class Case02_JdbcSinkExactlyOnce {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置checkpoint
//        env.enableCheckpointing(1000);
//
//        //socket :005,188,187,busy,5000,50
//        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
//        //对ds进行转换
//        SingleOutputStreamOperator<StationLog> ds2 = ds.map(new MapFunction<String, StationLog>() {
//            @Override
//            public StationLog map(String line) throws Exception {
//                String[] split = line.split(",");
//                String sid = split[0];
//                String callOut = split[1];
//                String callIn = split[2];
//                String callType = split[3];
//                Long callTime = Long.valueOf(split[4]);
//                Long duration = Long.valueOf(split[5]);
//                return new StationLog(sid, callOut, callIn, callType, callTime, duration);
//            }
//        });
//
//        //准备JDBC Sink 对象
//        /**
//         *
//         * CREATE TABLE `station_log` (
//         *   `sid` varchar(255) DEFAULT NULL,
//         *   `call_out` varchar(255) DEFAULT NULL,
//         *   `call_in` varchar(255) DEFAULT NULL,
//         *   `call_type` varchar(255) DEFAULT NULL,
//         *   `call_time` bigint(20) DEFAULT NULL,
//         *   `duration` bigint(20) DEFAULT NULL
//         * ) ;
//         *
//         * JdbcSink.sink(
//         * sqlDmlStatement,      // 必须指定，SQL语句
//         * jdbcStatementBuilder, // 必须指定，给SQL语句设置参数
//         * jdbcExecutionOptions, // 可选，指定写出参数，如：提交周期、提交批次大小、重试时间，建议指定。
//         * jdbcConnectionOptions // 必须指定，数据库连接参数
//         * );
//         */
//        SinkFunction<StationLog> jdbcSink = JdbcSink.exactlyOnceSink(
//                "insert into station_log (sid,call_out,call_in,call_type,call_time,duration) values (?,?,?,?,?,?)",
//                new JdbcStatementBuilder<StationLog>() {
//                    @Override
//                    public void accept(PreparedStatement pst, StationLog stationLog) throws SQLException {
//                        pst.setString(1, stationLog.sid);
//                        pst.setString(2, stationLog.callOut);
//                        pst.setString(3, stationLog.callIn);
//                        pst.setString(4, stationLog.callType);
//                        pst.setLong(5, stationLog.callTime);
//                        pst.setLong(6, stationLog.duration);
//                    }
//                },
//                JdbcExecutionOptions.builder()
//                        //向数据库提交批次的间隔，默认0
//                        .withBatchIntervalMs(1000)
//                        //数据库提交的批次大小，默认500
//                        .withBatchSize(1000)
//                        //连接重试次数，默认3
//                        .withMaxRetries(0)
//                        .build(),
//                JdbcExactlyOnceOptions.builder()
//                        //允许连接使用事务
//                        .withTransactionPerConnection(true)
//                        .build(),
//                new SerializableSupplier<XADataSource>() {
//                    @Override
//                    public XADataSource get() {
//                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
//                        xaDataSource.setURL("jdbc:mysql://node2:3306/mydb?useSSL=false");
//                        xaDataSource.setUser("root");
//                        xaDataSource.setPassword("123456");
//                        return xaDataSource;
//                    }
//                });
//
//        //数据写出到数据库
//        ds2.addSink(jdbcSink);
//        env.execute();
//    }
//}
