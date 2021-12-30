package com.yw.flume;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author yangwei
 */
public class MyInterceptor implements Interceptor {
    /**
     * 指定需要加密的字段下标
     */
    private final Integer encryptedFieldIndex;
    /**
     * 指定不需要对应列的下标
     */
    private final Integer outIndex;

    public MyInterceptor(Integer encryptedFieldIndex, Integer outIndex) {
        this.encryptedFieldIndex = encryptedFieldIndex;
        this.outIndex = outIndex;
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        try {
            // 13901007610,male,30,sing,beijing
            String line = new String(event.getBody(), Charsets.UTF_8);
            String[] fields = line.split(",");

            StringBuilder newLine = new StringBuilder();
            for (int i = 0; i < fields.length; i++) {
                if (i == outIndex) continue;
                if (i == encryptedFieldIndex) newLine.append(md5(fields[i]));
                else newLine.append(fields[i]);
                newLine.append(" ");
            }
            event.setBody(newLine.toString().getBytes(Charsets.UTF_8));
            return event;
        } catch (Exception e) {
            return event;
        }
    }

    private static String md5(String plainText) {
        //定义一个字节数组
        byte[] secretBytes;
        try {
            // 生成一个MD5加密计算摘要
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 对字符串进行加密
            md.update(plainText.getBytes());
            // 获得加密后的数据
            secretBytes = md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("没有md5这个算法！");
        }
        // 将加密后的数据转换为16进制数字
        StringBuilder md5code = new StringBuilder(new BigInteger(1, secretBytes).toString(16));
        // 如果生成数字未满32位，需要前面补0
        for (int i = 0; i < 32 - md5code.length(); i++) {
            md5code.insert(0, "0");
        }
        return md5code.toString();
    }

    /**
     * 批量event拦截逻辑
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        list.forEach(this::intercept);
        return list.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public void close() {

    }

    /**
     * 相当于自定义Interceptor的工厂类
     * 在flume采集配置文件中通过指定该Builder来创建Interceptor对象
     * 可以在Builder中获取、解析flume采集配置文件中的拦截器Interceptor的自定义参数：
     * 指定需要加密的字段下标 指定需要剔除的对应列的下标等
     */
    public static class MyBuilder implements Interceptor.Builder {
        private Integer encryptedFieldIndex;
        private Integer outIndex;

        @Override
        public Interceptor build() {
            return new MyInterceptor(encryptedFieldIndex, outIndex);
        }

        @Override
        public void configure(Context context) {
            // 从flume的配置文件中获得拦截器的“encrypted_field_index”属性值
            this.encryptedFieldIndex = context.getInteger("encrypted_field_index", -1);
            // 从flume的配置文件中获得拦截器的“out_index”属性值
            this.outIndex = context.getInteger("out_index", -1);
        }
    }
}
