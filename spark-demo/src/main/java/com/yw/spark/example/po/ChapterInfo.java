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
public class ChapterInfo {
    private Integer course_id;
    private Integer chapter_id;
    private String chapter_name;
    private Integer rank_num;
    private List<SectionInfo> section_list;
}
