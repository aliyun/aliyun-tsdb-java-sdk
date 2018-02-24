package com.aliyun.hitsdb.client.tscompress.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aliyun.hitsdb.client.exception.BufferQueueFullException;
import com.aliyun.hitsdb.client.value.request.CompressionBatchPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCompressionBatchPointsQueue implements CompressionBatchPointsQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompressionBatchPointsQueue.class);
    private final BlockingQueue<CompressionBatchPoints> pointQueue;
    private final AtomicBoolean forbiddenWrite = new AtomicBoolean(false);
    private final int waitCloseTimeLimit;
    private final boolean backpressure;

    public DefaultCompressionBatchPointsQueue(int size,int waitCloseTimeLimit,boolean backpressure) {
        this.pointQueue = new ArrayBlockingQueue<CompressionBatchPoints>(size);
        this.waitCloseTimeLimit = waitCloseTimeLimit;
        this.backpressure = backpressure;
    }

    public void send(CompressionBatchPoints points) {
        if (forbiddenWrite.get()) {
            throw new IllegalStateException("client has been closed.");
        }
        
        if(this.backpressure){
            try {
                pointQueue.put(points);
            } catch (InterruptedException e) {
                LOGGER.error("Client Thread been Interrupted.",e);
                return;
            }
        } else {
            try {
                pointQueue.add(points);
            } catch(IllegalStateException exception) {
                throw new BufferQueueFullException("The buffer queue is full.",exception);
            }
        }
    }

    public CompressionBatchPoints receive() throws InterruptedException {
        CompressionBatchPoints points = null;
        points = pointQueue.take();
        return points;
    }

    public CompressionBatchPoints receive(int timeout) throws InterruptedException {
        CompressionBatchPoints points = pointQueue.poll(timeout, TimeUnit.MILLISECONDS);
        return points;
    }

    @Override
    public void forbiddenSend() {
        forbiddenWrite.compareAndSet(false, true);
    }

    @Override
    public void waitEmpty() {
        // 等待为空之前，必须已经设置了禁止写入
        if (forbiddenWrite.get()) {
            try {
                Thread.sleep(waitCloseTimeLimit);
            } catch (InterruptedException e) {
                LOGGER.warn("The method waitEmpty() is being illegally interrupted");
            }
            
            while (true) {
                boolean empty = pointQueue.isEmpty();
                if (empty) {
                    return ;
                } else {
                    try {
                        Thread.sleep(waitCloseTimeLimit);
                    } catch (InterruptedException e) {
                        LOGGER.warn("The waitEmpty() method is being illegally interrupted");
                    }
                }
            }
        } else {
            throw new IllegalStateException(
                    "The queue is still allowed to write data. you must first call the forbiddenSend() method");
        }
        
        
    }

    @Override
    public boolean isEmpty() {
        return pointQueue.isEmpty();
    }

}