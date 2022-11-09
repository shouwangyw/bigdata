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
public class Content {
    private String callback_key;

    private Integer material_id;
    private String url;
    private String name;

}
