package com.aliyun.hitsdb.client.queue;

public class DataQueueFactory {

    public static DataQueue createDataPointQueue(int batchPutBufferSize, int multiFieldBatchPutBufferSize, int waitTimeLimit, boolean backpressure) {
        return new DataPointQueue(batchPutBufferSize, multiFieldBatchPutBufferSize, waitTimeLimit, backpressure);
    }
}