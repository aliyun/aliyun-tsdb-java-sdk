package com.aliyun.hitsdb.client.queue;

public class DataQueueFactory {

    public static DataQueue createDataPointQueue(int size, int waitTimeLimit, boolean backpressure) {
        return new DataPointQueue(size, waitTimeLimit, backpressure);
    }
}