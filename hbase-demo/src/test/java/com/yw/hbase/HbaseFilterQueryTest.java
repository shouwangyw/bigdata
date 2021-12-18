package com.yw.hbase;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author yangwei
 */
public class HbaseFilterQueryTest {
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
     * 通过RowFilter过滤比rowKey = 0003小的所有值出来
     */
    @Test
    public void testRowFilter() throws IOException {
        Scan scan = new Scan().setFilter(
                /**
                 * rowFilter需要加上两个参数:
                 *      第一个参数就是我们的比较规则
                 *      第二个参数就是我们的比较对象
                 */
                new RowFilter(CompareOperator.LESS, new BinaryComparator("0003".getBytes()))
        );

        printResult(table.getScanner(scan));
    }

    /**
     * 通过 FamilyFilter 查询列族名包含f2的所有列族下面的数据
     */
    @Test
    public void testFamilyFilter() throws IOException {
        Scan scan = new Scan().setFilter(
                new FamilyFilter(CompareOperator.EQUAL, new SubstringComparator("f2"))
        );

        printResult(table.getScanner(scan));
    }

    /**
     * 通过 QualifierFilter 只查询列名包含`name`的列的值
     */
    @Test
    public void testQualifierFilter() throws IOException {
        Scan scan = new Scan().setFilter(
                new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator("name"))
        );

        printResult(table.getScanner(scan));
    }

    /**
     * 通过 ValueFilter 查询所有列当中包含8的数据
     */
    @Test
    public void testValueFilter() throws IOException {
        Scan scan = new Scan().setFilter(
                new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("8"))
        );

        printResult(table.getScanner(scan));
    }

    /**
     * 通过 SingleColumnValueFilter 查询 f1 列族 name 列值为 刘备 的数据
     */
    @Test
    public void testSingleColumnValueFilter() throws IOException {
        Scan scan = new Scan().setFilter(
                new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(),
                        CompareOperator.EQUAL, new SubstringComparator("刘备"))
        );

        printResult(table.getScanner(scan));
    }

    /**
     * 通过 SingleColumnValueExcludeFilter 查询排出了 f1 列族 name 列值为 刘备 的数据
     */
    @Test
    public void testSingleColumnValueExcludeFilter() throws IOException {
        Scan scan = new Scan().setFilter(
                new SingleColumnValueExcludeFilter("f1".getBytes(), "name".getBytes(),
                        CompareOperator.EQUAL, new SubstringComparator("刘备"))
        );

        printResult(table.getScanner(scan));
    }

    /**
     * 通过 PrefixFilter 查询以00开头的所有前缀的rowkey
     */
    @Test
    public void testPrefixFilter() throws IOException {
        Scan scan = new Scan().setFilter(
                new PrefixFilter("00".getBytes())
        );

        printResult(table.getScanner(scan));
    }

    /**
     * 通过 PageFilter 实现分页查询
     */
    @Test
    public void testPageFilter() throws IOException {
        int pageNum = 1, pageSize = 2;
        byte[] startRow = null;
        do {
            Scan scan = new Scan().setFilter(
                    new PageFilter(pageSize)
            ).withStartRow(startRow)
                    // 设置一步往前扫描多少条数据
                    .setMaxResultSize(pageSize);
            ResultScanner scanner = table.getScanner(scan);

            System.out.println("############################ 第 " + (pageNum++) + " 页 ############################");
            int i = 0;
            for (Result result : scanner) {
                printData(result.listCells());
                System.out.println("----------------------------------------------------------");
                startRow = result.getRow();
                i++;
            }
            if (startRow != null) startRow[startRow.length - 1]++;
            if (i < pageSize) startRow = null;
        } while (startRow != null);
    }

    /**
     * 通过 FilterList 组合多个过滤器
     *  实现SingleColumnValueFilter查询f1列族，name为刘备的数据，并且同时满足rowkey的前缀以00开头的数据（PrefixFilter）
     */
    @Test
    public void testFilterList() throws IOException {
        FilterList filterList = new FilterList();
        filterList.addFilter(Arrays.asList(
                new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareOperator.EQUAL, "刘备".getBytes()),
                new PrefixFilter("00".getBytes())
        ));
        Scan scan = new Scan().setFilter(filterList);

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
