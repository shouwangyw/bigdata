package com.yw.log.collector.service.impl;

import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author yangwei
 */
@Service("userPlaySongLogService")
public class UserPlaySongLogServiceImpl extends AbstractLogServiceImpl {
    public UserPlaySongLogServiceImpl() {
        super.log = LoggerFactory.getLogger(UserPlaySongLogServiceImpl.class);
    }
}
