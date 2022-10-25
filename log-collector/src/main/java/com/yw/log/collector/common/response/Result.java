package com.yw.log.collector.common.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author yangwei
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Result<T> implements Serializable {
    private T data;
    private boolean success;
    private String code;
    private String msg;

    public Result(boolean success, String code) {
        this.success = success;
        this.code = code;
    }

    public static Result<Void> success() {
        return success(null, null);
    }

    public static <R> Result<R> success(R data) {
        return success(data, null);
    }

    public static <R> Result<R> success(R data, String msg) {
        return new Result<>(data, true, RespCode.SUCCESS.getCode(), msg);
    }

    public static Result<Void> successMsg(String msg) {
        return success(null, msg);
    }

    public static <R> Result<R> failure(RespCode code) {
        return failure(code, null);
    }

    public static <R> Result<R> failure(RespCode code, String msg) {
        return failure(null, code, msg);
    }

    public static <R> Result<R> failure(R data, RespCode code, String msg) {
        return new Result<>(data, false, code.getCode(), msg);
    }

    public static <R> Result<R> failureMsg(String msg) {
        return failure(RespCode.UNKNOWN_FAIL, msg);
    }
}
