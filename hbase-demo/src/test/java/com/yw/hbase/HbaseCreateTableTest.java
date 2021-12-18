package com.yw.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;

/**
 * @author yangwei
 */
public class HbaseCreateTableTest {
    /**
     * 创建表 myuser，有两个列组 f1、f2
     * 步骤：1、获取连接
     * 2、获取客户端对象
     * 3、操作数据库
     * 4、关闭流
     */
    @Test
    public void createTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        // 指定hbase的zk集群地址
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 获取管理员对象，创建一张表
        Admin admin = connection.getAdmin();
        // 指定表名
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("myuser"))
                // 指定两个列族
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("f1".getBytes()).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("f2".getBytes()).build())
                .build();
        admin.createTable(tableDescriptor);
        // 关闭
        admin.close();
        connection.close();
    }
}
