package com.yw.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author yangwei
 */
public class HbaseInsertTest {
    private static final String TABLE_NAME = "myuser";
    private Connection connection;
    private Table table;

    @Before
    public void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    /**
     * 向myuser表中添加数据
     */
    @Test
    public void insertOne() throws IOException {
        // 创建 put 对象，并指定 rowkey
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("地球人"));
        table.put(put);
    }

    @Test
    public void batchInsert() throws IOException {
        // 创建 put 对象，并指定 rowkey
        Put put1 = new Put("0002".getBytes());
        put1.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put1.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("曹操"));
        put1.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
        put1.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("男"));
        put1.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("xxx"));
        put1.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put1.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("Hello"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("男"));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("yyy"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("How are you"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("男"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("zzz"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("18888888888"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("How old are you"));

        table.put(Arrays.asList(put1, put2, put3));
    }

    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }
}
