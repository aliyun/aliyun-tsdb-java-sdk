package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;

public class ConsumerFactory {

    public static Consumer createConsumer(TSDB tsdb, DataQueue buffer, HttpClient httpclient, RateLimiter rateLimiter, Config config) {
        DefaultBatchPutConsumer consumer = new DefaultBatchPutConsumer(tsdb, buffer, httpclient, rateLimiter, config);
        return consumer;
    }

}
