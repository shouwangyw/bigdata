package com.yw.spark.example.po;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
@ToString(callSuper = true)
public class CourseInfo {
    private Integer course_id;
    private String course_name;
    private String cover_img_path_web;
    private String cover_img_path_mobile;
    private String course_icon;
}
