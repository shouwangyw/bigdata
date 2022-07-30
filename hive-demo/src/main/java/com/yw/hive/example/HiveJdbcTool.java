package com.yw.hive.example;

import com.google.common.collect.Maps;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yangwei
 */
public class HiveJdbcTool {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL = "jdbc:hive2://node03:10000/metastore";

    public static void main(String[] args) throws Exception {
        Class.forName(HIVE_DRIVER);

        List<Map<String, Object>> results = getList(String.format("DESCRIBE %s.%s" , "datalake", "test_iceberg_tbl1"), 30 * 60);

        System.out.println(results);
    }

    public static List<Map<String, Object>> getList(String sql, Integer timeout) {
        if (sql.trim().endsWith(";")) {
            sql = sql.trim().substring(0, sql.trim().length() - 1);
        }
        List<Map<String, Object>> values = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(HIVE_JDBC_URL);
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setQueryTimeout(timeout);
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                // 获取字段
                int columnCount = rsmd.getColumnCount();
                List<String> columnName = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columnName.add(rsmd.getColumnLabel(i));
                }
                // 获取数据

                while (rs.next()) {
                    Map<String, Object> map = Maps.newLinkedHashMap();
                    for (String name : columnName) {
                        Object value = rs.getObject(name);
                        map.put(name, null != value ? value : "");
                    }
                    values.add(map);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return values;
    }
}