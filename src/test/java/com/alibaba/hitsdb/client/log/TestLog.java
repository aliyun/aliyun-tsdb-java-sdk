package com.alibaba.hitsdb.client.log;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLog {
    private Logger L = LoggerFactory.getLogger(TestLog.class);
    
    @Test
    public void testLogLevel(){
        L.trace("hello {}","world");
        L.debug("hello {}","world");
        L.info("hello {}","world");
        L.warn("hello {}","world");
        L.error("hello {}","world");
    }
}
