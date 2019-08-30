package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;

public class ConsumerFactory {

    public static Consumer createConsumer(DataQueue buffer, HttpClient httpclient, RateLimiter rateLimiter, Config config) {
        DefaultBatchPutConsumer consumer = new DefaultBatchPutConsumer(buffer, httpclient, rateLimiter, config);
        return consumer;
    }

}
