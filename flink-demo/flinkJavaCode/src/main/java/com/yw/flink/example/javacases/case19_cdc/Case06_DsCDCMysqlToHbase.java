package com.yw.flink.example.javacases.case19_cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

/**
 * Flink CDC 同步MySQL数据到HBase
 */
public class Case06_DsCDCMysqlToHbase {
    public static void main(String[] args) throws Exception {
        //创建 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("mysql_server")
                .port(3306)
                .databaseList("db1")
                .tableList("db1.tbl1")
                .username("root")
                .password("123456")
                //指定序列化格式：读取binlog数据转换成最终字符串的格式
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        //创建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置checkpoint
        env.enableCheckpointing(5000);

        //{"before":null,"after":{"id":10,"name":"zhangsan","age":30},"source":...,"op":"c","ts_ms":1697424054674,"transaction":null}
        //{"before":{"id":10,"name":"zhangsan","age":30},"after":{"id":10,"name":"zhangsan","age":30},"source":...,"op":"u","ts_ms":1697424054674,"transaction":null}
        //{"before":{"id":11,"name":"lisi","age":31},"after":null,"source":...,"op":"d","ts_ms":1697424054686,"transaction":null}
        SingleOutputStreamOperator<JSONObject> ds = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .map((MapFunction<String, JSONObject>) JSONObject::new);

        //将数据通过SinkFunction写出到Hbase
        ds.addSink(new RichSinkFunction<JSONObject>() {
            Connection connection = null;

            //在sink初始化时候连接Hbase
            @Override
            public void open(Configuration parameters) throws Exception {
                //配置
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "node3,node4,node5");
                conf.set("hbase.zookeeper.property.clientPort", "2181");

                //创建连接
                connection = ConnectionFactory.createConnection(conf);
            }

            //sink数据，每条数据调用一次
            @Override
            public void invoke(JSONObject currentOne, Context context) throws Exception {
                //{"before":null,"after":{"id":10,"name":"zhangsan","age":30},"source":...,"op":"c","ts_ms":1697424054674,"transaction":null}
                //{"before":{"id":10,"name":"zhangsan","age":30},"after":{"id":10,"name":"zhangsan","age":30},"source":...,"op":"u","ts_ms":1697424054674,"transaction":null}
                //{"before":{"id":11,"name":"lisi","age":31},"after":null,"source":...,"op":"d","ts_ms":1697424054686,"transaction":null}
                //获取操作
                String op = currentOne.getString("op");
                //插入、修改
                if ("c".equals(op) || "u".equals(op)) {
                    //获取after数据
                    JSONObject insertJsonObj = currentOne.getJSONObject("after");

                    //获取rowkey
                    String rowkey = insertJsonObj.getString("id");

                    //准备对应列
                    String id = rowkey;
                    String name = insertJsonObj.getString("name");
                    String age = insertJsonObj.getString("age");

                    //准备表对象
                    Table table = connection.getTable(TableName.valueOf("tbl_cdc"));

                    //创建put对象
                    Put p = new Put(Bytes.toBytes(rowkey));
                    //添加列
                    p.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("id"), Bytes.toBytes(id));
                    p.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(name));
                    p.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(age));

                    //插入数据
                    table.put(p);

                    //关闭表对象
                    table.close();


                }

                //删除
                if ("d".equals(op)) {

                    JSONObject deleteJsonObj = currentOne.getJSONObject("before");
                    String rowkey = deleteJsonObj.getString("id");
                    //准备表对象
                    Table table = connection.getTable(TableName.valueOf("tbl_cdc"));

                    Delete del = new Delete(Bytes.toBytes(rowkey));
                    del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("id"));
                    del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
                    del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));

                    //执行删除
                    table.delete(del);

                    //关闭对象
                    table.close();
                }
            }

            //释放资源
            @Override
            public void close() throws Exception {
                connection.close();
            }
        });

        env.execute();
    }
}
