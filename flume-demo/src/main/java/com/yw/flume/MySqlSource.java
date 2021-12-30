package com.yw.flume;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author yangwei
 */
public class MySqlSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger log = LoggerFactory.getLogger(MySqlSource.class);

    private MySqlHelper mySqlHelper;

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        // 初始化
        try {
            this.mySqlHelper = new MySqlHelper(context);
        } catch (ParseException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 接收mysql表中的数据
     */
    @Override
    public Status process() throws EventDeliveryException {
        try {
            // 查询数据表
            List<List<Object>> results = mySqlHelper.executeQuery();
            log.info("==> size = {}", results.size());
            // 如果有返回数据，则将数据封装为event
            if (CollectionUtils.isNotEmpty(results)) {
                // 存放event的集合
                List<Event> events = new ArrayList<>();
                List<String> allRows = mySqlHelper.getAllRows(results);
                for (String row : allRows) {
                    Event event = new SimpleEvent();
                    event.setBody(row.getBytes());
                    event.setHeaders(Collections.emptyMap());
                    events.add(event);
                }
                // 将event写入channel
                this.getChannelProcessor().processEventBatch(events);
                // 更新数据表中的offset信息
                mySqlHelper.updateOffset2DB(results.size());
            }
            // 等待时长
            Thread.sleep(mySqlHelper.getRunQueryDelay());
            return Status.READY;
        } catch (Exception e) {
            log.error(e.getMessage());
            return Status.BACKOFF;
        }
    }

    @Override
    public synchronized void stop() {
        log.info("Stopping sql source {} ...", getName());
        try {
            // 关闭资源
            mySqlHelper.close();
        } finally {
            super.stop();
        }
    }
}
