package com.yw.log.collector.service;

import javax.servlet.http.HttpServletRequest;

/**
 * Log服务接口
 * @author yangwei
 */
public interface LogService {
    /**
     * log请求处理统一入口
     */
    void process(HttpServletRequest request, String logType) throws Exception;

    /**
     * log日志
     */
    void log(String json, HttpServletRequest request, String logType) throws Exception;
}
