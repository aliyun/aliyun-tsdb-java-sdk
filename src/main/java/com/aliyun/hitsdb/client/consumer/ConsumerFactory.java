package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.google.common.util.concurrent.RateLimiter;

public class ConsumerFactory {

    public static Consumer createConsumer(DataQueue buffer, HttpClient httpclient, RateLimiter rateLimiter,HiTSDBConfig config) {
        DefaultBatchPutConsumer consumer = new DefaultBatchPutConsumer(buffer,httpclient,rateLimiter,config);
        return consumer;
    }

}
