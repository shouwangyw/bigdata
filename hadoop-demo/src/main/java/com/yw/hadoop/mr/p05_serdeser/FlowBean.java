package com.yw.hadoop.mr.p05_serdeser;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化与反序列化
 * @author yangwei
 */
public class FlowBean implements Writable {
    /**
     * 上行包个数
     */
    private int upPackNum;
    /**
     * 下行包个数
     */
    private int downPackNum;/**
     * 上行总流量
     */
    private int upPayLoad;/**
     * 下行总流量
     */
    private int downPayLoad;
    /**
     * 如果反序列化，则要用到
     */
    public FlowBean() {
    }
    @Override
    public void write(DataOutput out) throws IOException {
        // 调用序列化方法时，要用于类型匹配的write方法，并且要记住序列化顺序
        out.writeInt(upPackNum);
        out.writeInt(downPackNum);
        out.writeInt(upPayLoad);
        out.writeInt(downPayLoad);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 反序列化的顺序要与序列化保持一致，并使用匹配类型的方法
        this.upPackNum = in.readInt();
        this.downPackNum = in.readInt();
        this.upPayLoad = in.readInt();
        this.downPayLoad = in.readInt();
    }

    public int getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(int upPackNum) {
        this.upPackNum = upPackNum;
    }

    public int getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(int downPackNum) {
        this.downPackNum = downPackNum;
    }

    public int getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(int upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public int getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(int downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upPackNum=" + upPackNum +
                ", downPackNum=" + downPackNum +
                ", upPayLoad=" + upPayLoad +
                ", downPayLoad=" + downPayLoad +
                '}';
    }
}
