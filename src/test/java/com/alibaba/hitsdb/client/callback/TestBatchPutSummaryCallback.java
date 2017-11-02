package com.alibaba.hitsdb.client.callback;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.HiTSDB;
import com.alibaba.hitsdb.client.HiTSDBClientFactory;
import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.exception.VIPClientException;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.value.request.Point;
import com.alibaba.hitsdb.client.value.response.batch.SummaryResult;

public class TestBatchPutSummaryCallback {
    HiTSDB tsdb;

    @Before
    public void init() throws VIPClientException, HttpClientInitException, IOException {
        System.out.println("按下任意键，开始运行...");
        while (true) {
            int read = System.in.read();
            if (read != 0) {
                break;
            }
        }
        System.out.println("开始运行");
        
        BatchPutSummaryCallback pcb = new BatchPutSummaryCallback() {

            @Override
            public void response(String address,List<Point> input, SummaryResult result) {
                int success = result.getSuccess();
                int failed = result.getFailed();
                System.out.println(success + "," + failed);
            }
            
        };
        
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .httpConnectionPool(100)
                .listenBatchPut(pcb)
                .config();
        
        tsdb = HiTSDBClientFactory.connect(config);
    }
    
    @Test
    public void test() throws InterruptedException {
        for(int i = 0;i<1000;i++){
            Point point = createPoint(i%4,1.123);
            tsdb.put(point);
            Thread.sleep(1000);
        }
    }
    
    public Point createPoint(int tag, double value) {
        int t = (int) (System.currentTimeMillis() / 1000);
        return Point.metric("test-performance").tag("tag", String.valueOf(tag)).value(t, value).build();
    }
}
