package com.yw.log.collector.common.exception;

import com.yw.log.collector.common.response.RespCode;
import org.apache.commons.lang3.StringUtils;

/**
 * 自定义异常
 * @author yangwei
 */
public class CustomException extends RuntimeException {
    private static final long serialVersionVID = 1L;
    private RespCode respCode;

    public CustomException() {
        this.respCode = RespCode.UNKNOWN_FAIL;
    }

    public CustomException(Throwable cause) {
        super(cause);
        this.respCode = RespCode.UNKNOWN_FAIL;
    }

    public CustomException(RespCode respCode, Throwable cause) {
        super(cause);
        this.respCode = respCode;
    }

    public CustomException(RespCode respCode, String errorMsg, Throwable cause) {
        super(errorMsg, cause);
        this.respCode = respCode;
    }

    public CustomException(RespCode respCode) {
        this.respCode = respCode;
    }

    public CustomException(RespCode respCode, String errorMsg) {
        super(errorMsg);
        this.respCode = respCode;
    }

    public RespCode getRespCode() {
        return respCode;
    }

    public String getErrorCode() {
        return respCode == null ? StringUtils.EMPTY : respCode.getCode();
    }

    @Override
    public String getMessage() {
        return respCode.getMsg() + "\n" + super.getMessage();
    }

    @Override
    public String toString() {
        String s = this.getClass().getName();
        String errorCodeMsg = getErrorCode();
        String message = this.getLocalizedMessage();
        message = message == null ? errorCodeMsg : errorCodeMsg + message;
        return "".equals(message) ? s : s + ": " + message;
    }
}
