package com.yw.recommend.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yangwei
 */
public class DateUtils {
    // 获取时间粒度到分钟级别
    public static String getMin(Date date){
        String pattern = "yyyyMMddHHmm";
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(date);
    }
}
