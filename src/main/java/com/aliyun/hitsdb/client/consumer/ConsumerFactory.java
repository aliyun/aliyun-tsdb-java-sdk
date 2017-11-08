package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;

public class ConsumerFactory {

    public static Consumer createConsumer(DataQueue buffer, HttpClient httpclient, HiTSDBConfig config) {
        DefaultBatchPutConsumer consumer = new DefaultBatchPutConsumer(buffer,httpclient,config);
        return consumer;
    }

}
