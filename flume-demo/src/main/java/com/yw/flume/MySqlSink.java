package com.yw.flume;

import com.google.common.base.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author yangwei
 */
public class MySqlSink extends AbstractSink implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(MySqlSink.class);

    private String url;
    private String username;
    private String password;
    private String tableName;

    private transient Connection conn;

    /**
     * 获取配置文件中指定名称的参数值
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        /*
         getString的参数与agent配置文件中sink组件的自定义的属性名一一对应
          如context.getString("mysql_url");对应
          a1.sinks.k1.mysql_url=jdbc:mysql://node01:3306/mysqlsource?useSSL=false
         */
        this.url = context.getString("mysql_url");
        this.username = context.getString("username");
        this.password = context.getString("password");
        this.tableName = context.getString("table_name");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        tx.begin();
        Status status = Status.BACKOFF;
        try {
            Event event = channel.take();
            if (event != null) {
                // 获取body中的数据
                String body = new String(event.getBody(), Charsets.UTF_8);
                // 如果日志中有以下关键字的不需要保存，过滤掉
                if (!StringUtils.containsAny(body, "delete", "drop", "alert")) {
                    // 否则存入Mysql
                    String sql = "insert into " + tableName + " (content) values (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, body);
                        ps.execute();
                        status = Status.READY;
                    }
                }
            }
            tx.commit();
        } catch (Exception e) {
            log.error(e.getMessage());
            tx.rollback();
        } finally {
            tx.close();
        }
        return status;
    }

    @Override
    public synchronized void start() {
        try {
            this.conn = DriverManager.getConnection(url, username, password);
            super.start();
            log.info("finish start");
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public synchronized void stop() {
        try {
            if (conn != null) {
                conn.close();
            }
            super.stop();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
