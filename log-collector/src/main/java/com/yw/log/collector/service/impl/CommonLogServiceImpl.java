package com.yw.log.collector.service.impl;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author yangwei
 */
@Service("commonLogService")
public class CommonLogServiceImpl extends AbstractLogServiceImpl {
    public CommonLogServiceImpl() {
        super.log = LoggerFactory.getLogger(CommonLogServiceImpl.class);
    }
}
