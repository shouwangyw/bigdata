package com.yw.flink.example.javacases.case04_sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Flink 通过自定义Sink 实现数据写出到HBase
 * 案例：读取Socket 基站日志数据，写出到HBase
 * 001,186,187,success,1000,10
 */
public class Case05_CustomSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> result = env.socketTextStream("node5", 9999);
        //自定义Sink 写出到HBase 中
        result.addSink(new RichSinkFunction<String>() {
            Connection conn = null;

            //初始化资源
            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "node3,node4,node5");
                conf.set("hbase.zookeeper.prperty.clientPort", "2181");
                conn = ConnectionFactory.createConnection(conf);
            }

            //写出一条数据的逻辑
            @Override
            public void invoke(String currentOne, Context context) throws Exception {
                //001,186,187,success,1000,10
                String[] split = currentOne.split(",");
                String sid = split[0];
                String callOut = split[1];
                String callIn = split[2];
                //准备rowKey
                String rowKey = sid + callOut + callIn;
                String callType = split[3];
                String callTime = split[4];
                String duration = split[5];

                //获取HBase 表对象
                Table table = conn.getTable(TableName.valueOf("flink-sink-hbase"));
                //创建put对象
                Put put = new Put(Bytes.toBytes(rowKey));
                //准备列族下的列
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("callOut"), Bytes.toBytes(callOut));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("callIn"), Bytes.toBytes(callIn));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("callType"), Bytes.toBytes(callType));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("callTime"), Bytes.toBytes(callTime));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("duration"), Bytes.toBytes(callTime));

                //插入数据
                table.put(put);
                table.close();
            }

            //关闭资源
            @Override
            public void close() throws Exception {
                conn.close();
            }
        });

        env.execute();

    }
}
