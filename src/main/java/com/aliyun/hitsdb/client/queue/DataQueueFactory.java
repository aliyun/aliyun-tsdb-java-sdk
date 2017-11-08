package com.aliyun.hitsdb.client.queue;

public class DataQueueFactory {

    public static DataQueue createDataPointQueue(int size,int waitTimeLimit,boolean backpressure) {
        DataQueue instance = new DataPointQueue(size,waitTimeLimit,backpressure);
        return instance;
    }
}