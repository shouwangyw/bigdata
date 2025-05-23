package com.yw.log.collector.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yw.log.collector.common.exception.CustomException;
import com.yw.log.collector.common.response.RespCode;
import com.yw.log.collector.service.LogService;
import com.yw.log.collector.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * @author yangwei
 */
public class AbstractLogServiceImpl implements LogService {
    protected Logger log;

    private static final int MAX_TRY_TIMES = 100;

    @Override
    public void process(HttpServletRequest request, String logType) {
        if (StringUtils.isBlank(logType)) {
            throw new CustomException(RespCode.ERR_UPLOAD_LOG_TYPE);
        }
        int contentLength = request.getContentLength();
        if (contentLength < 1) {
            throw new CustomException(RespCode.INVALID_RESQUEST_CONTENT);
        }
        byte[] bytes = new byte[contentLength];
        try (BufferedInputStream bis = new BufferedInputStream(request.getInputStream())){
            // 最大尝试读取次数
            int tryTimes = 0;
            // 最大尝试读取次数内最终读取数据长度
            int totalReadLength = 0;
            // 保证读取输入流里所有数据，最大尝试读取数据时长为20s
            while (totalReadLength < contentLength && tryTimes < MAX_TRY_TIMES) {
                int readLength = bis.read(bytes, totalReadLength, contentLength - totalReadLength);
                if (readLength < 0) {
                    throw new CustomException(RespCode.BAD_NETWORK, logType);
                }
                totalReadLength += readLength;
                if (totalReadLength == contentLength) {
                    break;
                }
                tryTimes++;
                // 每次尝试后延时200ms，最大尝试时长为(100*200)ms
                TimeUnit.MILLISECONDS.sleep(200);
            }
            // 经过多次尝试读取输入流数据仍然未读取完整的，则判定为网络欠佳异常
            if (totalReadLength < contentLength) {
                throw new CustomException(RespCode.BAD_NETWORK, logType);
            }
        } catch (Exception e) {
            throw new CustomException(RespCode.BAD_NETWORK, e);
        }
        // 处理数据
        this.log(new String(bytes, StandardCharsets.UTF_8), request, logType);
    }

    @Override
    public void log(String json, HttpServletRequest request, String logType) {
        JSONArray jsonArray = CommonUtils.checkJsonArray(json);
        if (jsonArray.size() <= 0) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0, size = jsonArray.size(); i < size; i++) {
            JSONObject ele = jsonArray.getJSONObject(i);
            sb.append(ele.toString()).append(i == size - 1 ? StringUtils.EMPTY : ",");
        }
        try {
            log.info("{}", sb);
        } catch (Exception e) {
            throw new CustomException(RespCode.LOG_FAIL, logType);
        }
    }
}
