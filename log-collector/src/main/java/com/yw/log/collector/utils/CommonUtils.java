package com.yw.log.collector.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.yw.log.collector.common.exception.CustomException;
import com.yw.log.collector.common.response.RespCode;

/**
 * 日志处理工具类
 *
 * @author yangwei
 */
public class CommonUtils {
    private CommonUtils() {}
    /**
     * 检测参数
     */
    public static JSONArray checkJsonArray(String json) {
        try {
            return JSON.parseArray(json);
        } catch (Exception e) {
            throw new CustomException(RespCode.JSON_PARSE_FAIL, json);
        }
    }
}
