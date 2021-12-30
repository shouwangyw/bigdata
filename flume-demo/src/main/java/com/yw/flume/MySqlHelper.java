package com.yw.flume;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author yangwei
 */
public class MySqlHelper {
    private static final Logger log = LoggerFactory.getLogger(MySqlHelper.class);

    private int runQueryDelay,      //两次查询的时间间隔
            startFrom,              //开始id
            currentIndex,           //当前id
            recordSixe = 0,         //每次查询返回结果的条数
            maxRow;                 //每次查询的最大条数

    private String table,           //要操作的表
            columnsToSelect,        //用户传入的查询的列
            customQuery,            //用户传入的查询语句
            query,                  //构建的查询语句
            defaultCharsetResultSet;//编码集

    /**
     * 上下文，用来获取配置文件
     */
    private Context context;

    /**
     * 为定义的变量赋值（默认值），可在flume任务的配置文件中修改
     */
    private static final int DEFAULT_QUERY_DELAY = 10000;
    private static final int DEFAULT_START_VALUE = 0;
    private static final int DEFAULT_MAX_ROWS = 2000;
    private static final String DEFAULT_COLUMNS_SELECT = "*";
    private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";

    private static Connection conn = null;
    /**
     * 连接mysql的url、用户名、密码
     */
    private static String url, username, password;

    /**
     * 加载静态资源
     */
    static {
        Properties prop = new Properties();
        try {
            prop.load(MySqlHelper.class.getClassLoader().getResourceAsStream("jdbc.properties"));
            url = prop.getProperty("dbUrl");
            username = prop.getProperty("dbUser");
            password = prop.getProperty("dbPassword");
            Class.forName(prop.getProperty("dbDriver"));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    public MySqlHelper(Context context) throws ParseException {
        // 初始化上下文
        this.context = context;
        // 有默认值参数：获取flume任务配置文件中的参数，读不到的采用默认值
        this.columnsToSelect = context.getString("columns.to.select", DEFAULT_COLUMNS_SELECT);
        this.runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
        this.startFrom = context.getInteger("start.from", DEFAULT_START_VALUE);
        this.defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);
        // 无默认值参数：获取flume任务配置文件中的参数
        this.table = context.getString("table");
        this.customQuery = context.getString("custom.query");

        url = context.getString("connection.url", url);
        username = context.getString("connection.user", username);
        password = context.getString("connection.password", password);

        // 校验相应的配置信息，如果没有默认值的参数也没赋值，抛出异常
        checkMandatoryProperties();

        initConnection();

        // 从要读取数据的表中，获取之前数据已经读到id为几？
        currentIndex = getStatusDBIndex();
        // 构建查询语句
        query = buildQuery();
        log.info("sql: {}", query);
    }

    /**
     * 初始化连接
     */
    private static void initConnection() {
        try {
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 校验相应的配置信息（表，查询语句以及数据库连接的参数）
     */
    private void checkMandatoryProperties() {
        if (table == null) {
            throw new ConfigurationException("property table not set");
        }
        if (url == null) {
            throw new ConfigurationException("connection.url property not set");
        }
        if (username == null) {
            throw new ConfigurationException("connection.user property not set");
        }
        if (password == null) {
            throw new ConfigurationException("connection.password property not set");
        }
    }

    /**
     * 获取当前id的offset
     */
    private Integer getStatusDBIndex() {
        // 从 flume_meta 表中查询出当前的id是多少
        String dbIndex = queryOne("select currentIndex from flume_meta where source_tab='" + table + "'");
        if (dbIndex != null) {
            return Integer.parseInt(dbIndex);
        }
        // 如果没有数据，则说明是第一次查询或者数据表中还没有存入数据，返回最初值
        return startFrom;
    }

    /**
     * 查询一条数据的执行语句(当前id)
     */
    private String queryOne(String sql) {
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                return rs.getString(1);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return StringUtils.EMPTY;
    }

    /**
     * 构建sql语句
     */
    private String buildQuery() {
        // 获取当前id
        currentIndex = getStatusDBIndex();
        log.info("currentIndex = {}", currentIndex);

        String sql = customQuery == null ? ("SELECT " + columnsToSelect + " FROM " + table) : customQuery;
        StringBuilder execSql = new StringBuilder(sql);
        //以id作为offset
        if (!sql.contains("where")) {
            execSql.append(" where ");
            execSql.append("id").append(" > ").append(currentIndex);
            return execSql.toString();
        } else {
            int length = execSql.toString().length();
            return execSql.toString().substring(0, length - String.valueOf(currentIndex).length()) + currentIndex;
        }
    }

    /**
     * 执行查询
     */
    public List<List<Object>> executeQuery() {
        //每次执行查询时都要重新生成sql，因为id不同
        customQuery = buildQuery();
        log.info("sql: {}", customQuery);
        //存放结果的集合

        try (PreparedStatement ps = conn.prepareStatement(customQuery);
             ResultSet rs = ps.executeQuery(customQuery)) {
            List<List<Object>> results = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    row.add(rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        } catch (SQLException e) {
            log.error(e.getMessage());
            // 重连
            initConnection();
        }
        return Collections.emptyList();
    }

    /**
     * 将结果集转化为字符串，每一条数据是一个list集合，将每一个小的list集合转化为字符串
     */
    public List<String> getAllRows(List<List<Object>> queryResult) {
        if (CollectionUtils.isEmpty(queryResult)) {
            return Collections.emptyList();
        }
        List<String> allRows = new ArrayList<>();
        StringBuilder row = new StringBuilder();
        for (List<Object> results : queryResult) {
            for (Object result : results) {
                if (result != null) row.append(result.toString());
                row.append(",");
            }
            allRows.add(row.toString());
            row = new StringBuilder();
        }
        return allRows;
    }

    /**
     * 更新offset元数据状态，每次返回结果集后调用。必须记录每次查询的offset值，为程序中断续跑数据时使用，以id为offset
     */
    public void updateOffset2DB(int size) {
        // 以source_tab做为KEY，如果不存在则插入，存在则更新（每个源表对应一条记录）
        String sql = "insert into flume_meta(source_tab,currentIndex) VALUES('"
                + this.table
                + "','" + (recordSixe += size)
                + "') on DUPLICATE key update source_tab=values(source_tab),currentIndex=values(currentIndex)";
        log.info("updateStatus Sql:{}", sql);
        execSql(sql);
    }

    /**
     * 执行sql语句
     */
    private void execSql(String sql) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            log.info("exec:{}", sql);
            ps.execute();
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
    }


    public void close() {
        try {
            conn.close();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    int getCurrentIndex() {
        return currentIndex;
    }

    void setCurrentIndex(int newValue) {
        currentIndex = newValue;
    }

    public int getRunQueryDelay() {
        return runQueryDelay;
    }

    String getQuery() {
        return query;
    }

    String getUrl() {
        return url;
    }

    private boolean isCustomQuerySet() {
        return (customQuery != null);
    }

    Context getContext() {
        return context;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    String getDefaultCharsetResultSet() {
        return defaultCharsetResultSet;
    }
}
