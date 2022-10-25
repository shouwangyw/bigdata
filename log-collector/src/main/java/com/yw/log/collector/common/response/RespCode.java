package com.yw.log.collector.common.response;

/**
 * @author yangwei
 */
public enum RespCode {
    /**
     *
     */
    SUCCESS("0000", "success"),
    UNKNOWN_FAIL("0001", "unknown error"),
    ERR_UPLOAD_LOG_TYPE("0002", "wrong upload logType"),
    ERR_COMPERSS_FLAG("0003", "wrong compress flag"),
    INVALID_RESQUEST_CONTENT("0004", "invalid request content"),
    DECOMPRESS_FAIL("0005", "decompress fail"),
    JSON_PARSE_FAIL("0006", "json parse fail"),
    LOG_FAIL("0007", "log fail"),
    BAD_NETWORK("0008", "bad network"),
    ERR_SING_CHECK("0009", "sign check error"),
    ;

    private String code;
    private String msg;

    RespCode(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public String getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }
}
