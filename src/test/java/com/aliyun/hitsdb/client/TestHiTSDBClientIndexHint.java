package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Copyright @ 2020 alibaba.com
 * All right reserved.
 * Function：Test HiTSDB Client Index Hint
 *
 * @author Benedict Jin
 * @since 2020-8-12
 */
public class TestHiTSDBClientIndexHint {

    private TSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        TSDBConfig config = TSDBConfig
                .address("localhost", 8242)
                .config();
        tsdb = TSDBClientFactory.connect(config);
    }

    @Test
    public void test() throws Exception {
        tsdb.disableIndex(JSON.parseObject("{\"TestHiTSDBClientIndexHint1\":[\"tagk1\",\"tagk2\",\"tagk3\"],\"TestHiTSDBClientIndexHint2\":[\"tagk4\",\"tagk5\",\"tagk6\"]}", new TypeReference<Map<String, Set<String>>>() {
        }));
        Thread.sleep(30000L);
        final Map<String, Set<String>> index = tsdb.getIndex();
        final Set<String> metric1 = index.get("TestHiTSDBClientIndexHint1");
        // ["tagk1","tagk2","tagk3"]
        System.out.println(JSON.toJSONString(metric1));
        final Set<String> metric2 = tsdb.getIndex("TestHiTSDBClientIndexHint2");
        // ["tagk4","tagk5","tagk6"]
        System.out.println(JSON.toJSONString(metric2));
    }

    @After
    public void after() {
        try {
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
