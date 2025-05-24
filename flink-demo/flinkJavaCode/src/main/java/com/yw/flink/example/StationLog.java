package com.yw.flink.example;

/**
 * 基站日志类
 * sid: 基站id
 * callOut : 主叫号码
 * callIn : 被叫号码
 * callType : 通话类型，fail busy barring success
 * callTime : 呼叫时间
 * duration : 通话时长
 */
public class StationLog {
    public String sid;
    public String callOut;
    public String callIn;
    public String callType;
    public Long callTime;
    public Long duration;

    public StationLog() {
    }

    public StationLog(String sid, String callOut, String callIn, String callType, Long callTime, Long duration) {
        this.sid = sid;
        this.callOut = callOut;
        this.callIn = callIn;
        this.callType = callType;
        this.callTime = callTime;
        this.duration = duration;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public void setCallOut(String callOut) {
        this.callOut = callOut;
    }

    public void setCallIn(String callIn) {
        this.callIn = callIn;
    }

    public void setCallType(String callType) {
        this.callType = callType;
    }

    public void setCallTime(Long callTime) {
        this.callTime = callTime;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public String getSid() {
        return sid;
    }

    public String getCallOut() {
        return callOut;
    }

    public String getCallIn() {
        return callIn;
    }

    public String getCallType() {
        return callType;
    }

    public Long getCallTime() {
        return callTime;
    }

    public Long getDuration() {
        return duration;
    }

    @Override
    public String toString() {
        return "StationLog{" +
                "sid='" + sid + '\'' +
                ", callOut='" + callOut + '\'' +
                ", callIn='" + callIn + '\'' +
                ", callType='" + callType + '\'' +
                ", callTime=" + callTime +
                ", duration=" + duration +
                '}';
    }
}
