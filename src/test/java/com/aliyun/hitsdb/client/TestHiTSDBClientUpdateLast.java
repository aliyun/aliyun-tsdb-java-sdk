package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestHiTSDBClientUpdateLast {
    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .config();
        
        tsdb = HiTSDBClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetDataPointStatus() {
        System.out.println(tsdb.getLastDataPointStatus());
    }


    @Test
    public void testUpdateLastDataPointStatus() {
        boolean rowFlag = tsdb.getLastDataPointStatus();
        System.out.println(rowFlag);
        // 更新
        System.out.println("update:" + tsdb.updateLastDataPointStatus(!rowFlag));
        System.out.println(tsdb.getLastDataPointStatus());
        // 还原
        System.out.println("update:" + tsdb.updateLastDataPointStatus(rowFlag));
        //
        assertEquals(rowFlag,tsdb.getLastDataPointStatus());
        System.out.println(tsdb.getLastDataPointStatus());
    }
    
}
