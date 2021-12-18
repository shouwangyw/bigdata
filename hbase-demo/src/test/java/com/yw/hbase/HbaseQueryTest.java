package com.yw.hbase;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author yangwei
 */
public class HbaseQueryTest {
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
     * 查询rowkey为0003的数据
     */
    @Test
    public void getData() throws IOException {
        Get get = new Get(Bytes.toBytes("0003"));
        // 限制只查询f1列族下面所有列的值
        get.addFamily("f1".getBytes());
        // 查询f2  列族 phone  这个字段
        get.addColumn("f2".getBytes(), "phone".getBytes());
        // 通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面了
        Result result = table.get(get);
        // 获取一条数据所有的cell，所有数据值都是在cell里面
        printData(result.listCells());
    }

    /**
     * 不知道rowkey的具体值，想查询rowkey范围是0001到0003
     */
    @Test
    public void scanData() throws IOException {
        Scan scan = new Scan();
        scan.addFamily("f1".getBytes());
        scan.addColumn("f2".getBytes(), "phone".getBytes());
        scan.withStartRow("0001".getBytes()).withStopRow("0004".getBytes()); // 左闭右开

        printResult(table.getScanner(scan));
    }

    private void printResult(ResultScanner scanner) {
        for (Result result : scanner) {
            printData(result.listCells());
            System.out.println("----------------------------------------------------------");
        }
    }

    private void printData(List<Cell> cells) {
        if (CollectionUtils.isEmpty(cells)) {
            return;
        }
        System.out.printf("%-15s%-15s%-15s%-15s\n", "rowKey", "familyName", "columnName", "cellValue");
        for (Cell cell : cells) {
            // 获取rowKey
            byte[] rowKey = CellUtil.cloneRow(cell);
            // 获取列族名
            byte[] familyName = CellUtil.cloneFamily(cell);
            // 获取列名
            byte[] columnName = CellUtil.cloneQualifier(cell);
            // 获取cell值
            byte[] cellValue = CellUtil.cloneValue(cell);
            // 需要判断字段的数据类型，使用对应的转换的方法，才能够获取到值
            if ("age".equals(Bytes.toString(columnName)) || "id".equals(Bytes.toString(columnName))) {
                System.out.printf("%-15s%-15s%-15s%-15s\n", Bytes.toString(rowKey), Bytes.toString(familyName),
                        Bytes.toString(columnName), Bytes.toInt(cellValue));
            } else {
                System.out.printf("%-15s%-15s%-15s%-15s\n", Bytes.toString(rowKey), Bytes.toString(familyName),
                        Bytes.toString(columnName), Bytes.toString(cellValue));
            }
        }
    }

    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }
}
