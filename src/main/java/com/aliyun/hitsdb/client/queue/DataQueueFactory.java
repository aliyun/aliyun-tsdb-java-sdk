package com.aliyun.hitsdb.client.queue;

import com.aliyun.hitsdb.client.tscompress.queue.CompressionBatchPointsQueue;
import com.aliyun.hitsdb.client.tscompress.queue.DefaultCompressionBatchPointsQueue;

public class DataQueueFactory {

    public static DataQueue createDataPointQueue(int size,int waitTimeLimit,boolean backpressure) {
        DataQueue instance = new DataPointQueue(size,waitTimeLimit,backpressure);
        return instance;
    }
    
    public static CompressionBatchPointsQueue createCompressionBatchPointsQueue(int size,int waitTimeLimit,boolean backpressure) {
        CompressionBatchPointsQueue instance = new DefaultCompressionBatchPointsQueue(size,waitTimeLimit,backpressure);
        return instance;
    }
}