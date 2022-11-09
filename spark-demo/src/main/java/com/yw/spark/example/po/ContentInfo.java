package com.yw.spark.example.po;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * @author yangwei
 */
@Data
@Accessors(chain = true)
@ToString(callSuper = true)
public class ContentInfo {
    private Integer course_id;
    private Integer chapter_id;
    private Integer section_id;
    private Integer group_id;
    private Integer content_id;
    private String content_title;
    private List<Content> content;
}
