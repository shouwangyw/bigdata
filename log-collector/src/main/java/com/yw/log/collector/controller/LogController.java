package com.yw.log.collector.controller;

import com.yw.log.collector.common.constant.LogType;
import com.yw.log.collector.common.response.RespCode;
import com.yw.log.collector.common.response.Result;
import com.yw.log.collector.service.LogService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * 日志采集控制器
 * @author yangwei
 */
@RestController
@RequestMapping("/collector")
public class LogController {
    @Autowired
    private LogService commonLogService;
    @Autowired
    private LogService userPlaySongLogService;

    @PostMapping("/common/{logType}")
    public Result<Void> collect(@PathVariable("logType") String logType, HttpServletRequest request) throws Exception {
        if (StringUtils.equals(logType, LogType.LOG_TYPE_USER_PLAY_SONG)) {
            userPlaySongLogService.process(request, logType);
        } else if (StringUtils.equals(logType, LogType.LOG_TYPE_USER_LOGIN)) {
            commonLogService.process(request, logType);
        } else {
            return Result.failure(RespCode.ERR_UPLOAD_LOG_TYPE);
        }
        return Result.success();
    }
}
