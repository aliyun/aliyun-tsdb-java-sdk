package com.aliyun.hitsdb.client.tscompress.queue;

import com.aliyun.hitsdb.client.value.request.CompressionBatchPoints;

public interface CompressionBatchPointsQueue {
    void send(CompressionBatchPoints point);

    CompressionBatchPoints receive() throws InterruptedException;

    CompressionBatchPoints receive(int timeout) throws InterruptedException;

    void forbiddenSend();

    void waitEmpty();
    
    boolean isEmpty();
}
