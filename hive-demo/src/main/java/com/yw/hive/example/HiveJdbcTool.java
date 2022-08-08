package com.yw.hive.example;

import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangwei
 */
public class HiveJdbcTool {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_JDBC_URL = "jdbc:hive2://node03:10000/default";
    private static final int DEFAULT_TIMEOUT = 30 * 60;

    public static void main(String[] args) throws Exception {
        Class.forName(HIVE_DRIVER);

        List<Map<String, Object>> dbMaps = getList("SHOW DATABASES");
        System.out.println("-------------- 1. 解析数据库信息 --------------");
        parseDbInfo(dbMaps);

        List<Map<String, Object>> tableMaps = getList("SHOW TABLES FROM datalake");
        System.out.println("\n-------------- 2. 解析数据表信息 --------------");
        parseTableInfo(tableMaps);

        List<Map<String, Object>> columnMaps = getList(String.format("DESCRIBE %s.%s", "datalake", "test_iceberg_tbl1"));
        System.out.println("\n-------------- 3. 解析数据表列信息 --------------");
        parseColumnInfo(columnMaps);

        System.out.println("\n-------------- 4. 执行 sql --------------");
        exec("insert into datalake.test_iceberg_tbl1 values (1, 'zhangsan', 32, '20220706'), (2, 'lisi', 24, '20220801');");

        System.out.println("\n-------------- 5. 执行 sql，并获取返回值 --------------");
        System.out.println(getList("select * from datalake.test_iceberg_tbl1"));
    }

    /**
     * 解析database 数据库信息
     */
    private static void parseDbInfo(List<Map<String, Object>> dbMaps) {
        for (Map<String, Object> map : dbMaps) {
            String dbName = objectToString(map.get("namespace"));
            System.out.println("dbName = " + dbName);
        }
    }

    /**
     * 解析table 表信息
     */
    private static void parseTableInfo(List<Map<String, Object>> tableMaps) {
        for (Map<String, Object> map : tableMaps) {
            // 获取表名称
            String tableName = getTableName(map);

            String lastColName = null;
            String colName = objectToString(map.get("col_name"));
            if (StringUtils.isNotBlank(colName)) {
                lastColName = colName;
            }
            // 获取表类型
            TableType tableType = TableType.OTHER;
            if (StringUtils.contains(colName, "Table Type:")) {
                tableType = TableType.getTableType(StringUtils.upperCase(objectToString(map.get("data_type"))));
            }
            if (StringUtils.contains(colName, "Provider")) {
                if (StringUtils.equalsIgnoreCase(objectToString(map.get("data_type")), "iceberg")) {
                    tableType = TableType.TABLE;
                }
            }
            // 获取表备注
            String tableComment = StringUtils.EMPTY;
            if (StringUtils.contains(lastColName, "Table Parameters:")) {
                String parameter = objectToString(map.get("data_type"));
                if (parameter != null && parameter.contains("comment")) {
                    tableComment = objectToString(map.get("comment"));
                }
            }

            System.out.println(
                    "tableName = " + tableName
                            + ", tableType = " + tableType.name()
                            + ", tableComment = " + tableComment
            );

        }
    }

    private static String getTableName(Map<String, Object> map) {
        String tableName = objectToString(map.get("tab_name"));
        if (tableName == null) {
            tableName = objectToString(map.get("tableName"));
        }
        if (tableName == null) {
            tableName = objectToString(map.get("table name"));
        }
        return tableName;
    }

    /**
     * 解析数据表column 列信息
     */
    private static void parseColumnInfo(List<Map<String, Object>> columnMaps) {
        for (Map<String, Object> map : columnMaps) {
            // 获取字段名
            String columnName = objectToString(map.get("col_name"));
            if (columnName == null) {
                columnName = objectToString(map.get("name"));
            }
            if (StringUtils.isBlank(columnName) || columnName.trim().startsWith("#")) {
                continue;
            }
            // 获取字段类型
            String columnType = objectToString(map.get("data_type"));
            if (columnType == null) {
                columnType = objectToString(map.get("type"));
            }
            // 获取字段备注
            String columnComment = objectToString(map.get("comment"));

            String defaultValue = objectToString(map.get("default_value"));
            String nullable = objectToString(map.get("nullable"));
            String elementType = objectToString(map.get("element_type"));
            Integer dimensionNum = objectToInteger(map.get("dimension_num"));

            System.out.println(
                    "columnName = " + columnName
                            + ", columnType = " + columnType
                            + ", columnComment = " + columnComment
                            + ", defaultValue = " + defaultValue
                            + ", nullable = " + nullable
                            + ", elementType = " + elementType
                            + ", dimensionNum = " + dimensionNum
            );
        }
    }

    private static String objectToString(Object object) {
        return object == null ? null : object.toString();
    }

    private static Integer objectToInteger(Object object) {
        return object == null ? null : Integer.valueOf(object.toString());
    }

    private static List<Map<String, Object>> getList(String sql) {
        if (sql.trim().endsWith(";")) {
            sql = sql.trim().substring(0, sql.trim().length() - 1);
        }
        List<Map<String, Object>> values = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(HIVE_JDBC_URL);
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setQueryTimeout(DEFAULT_TIMEOUT);
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
                    Map<String, Object> map = new HashMap<>(2);
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

    /**
     * 执行sql
     */
    public static void exec(String sql) {
        if (sql.trim().endsWith(";")) {
            sql = sql.trim().substring(0, sql.trim().length() - 1);
        }
        try (Connection connection = DriverManager.getConnection(HIVE_JDBC_URL);
             Statement stmt = connection.createStatement()) {
            stmt.setQueryTimeout(DEFAULT_TIMEOUT);
            stmt.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public enum TableType {
        /**
         *
         */
        TABLE,
        VIEW,
        FUNCTION,
        INDEX,
        OTHER,
        ;

        public static TableType getTableType(String value) {
            for (TableType type : TableType.values()) {
                if (StringUtils.containsIgnoreCase(value, type.name())) {
                    return type;
                }
            }
            return TableType.OTHER;
        }
    }
}