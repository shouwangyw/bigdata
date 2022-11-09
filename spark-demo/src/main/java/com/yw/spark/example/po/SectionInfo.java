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
public class SectionInfo {
    private Integer course_id;
    private Integer chapter_id;
    private Integer section_id;
    private String section_name;
    private Integer rank_num;
    private List<GroupInfo> group_list;
}
