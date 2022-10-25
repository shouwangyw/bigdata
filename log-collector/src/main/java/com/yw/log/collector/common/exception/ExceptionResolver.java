package com.yw.log.collector.common.exception;

import com.yw.log.collector.common.response.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author yangwei
 */
@Slf4j
@ControllerAdvice(annotations = Controller.class)
@ResponseBody
public class ExceptionResolver {
    @ExceptionHandler(Throwable.class)
    public Result<Void> exceptionHandler(Throwable e) {
        log.error("==>> Error: {}", e.getMessage());
        return Result.failureMsg("Error:" + e.getMessage());
    }

    @ExceptionHandler(CustomException.class)
    public Result<Void> customHandler(CustomException e) {
        log.error("==>> {}, {}", e.getErrorCode(), e.getMessage());
        return Result.failure(e.getRespCode(), e.getMessage());
    }
}
