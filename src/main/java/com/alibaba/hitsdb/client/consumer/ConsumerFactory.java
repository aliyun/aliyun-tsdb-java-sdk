package com.alibaba.hitsdb.client.consumer;

import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.http.HttpClient;
import com.alibaba.hitsdb.client.queue.DataQueue;

public class ConsumerFactory {

    public static Consumer createConsumer(DataQueue buffer, HttpClient httpclient, HiTSDBConfig config) {
        DefaultBatchPutConsumer consumer = new DefaultBatchPutConsumer(buffer,httpclient,config);
        return consumer;
    }

}
