package com.aliyun.hitsdb.client.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.hitsdb.client.exception.BufferQueueFullException;
import com.aliyun.hitsdb.client.value.request.Point;

public class DataPointQueue implements DataQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointQueue.class);
    private final BlockingQueue<Point> pointQueue;
    private final AtomicBoolean forbiddenWrite = new AtomicBoolean(false);
    private final int waitCloseTimeLimit;
    private final boolean backpressure;

    public DataPointQueue(int size,int waitCloseTimeLimit,boolean backpressure) {
        this.pointQueue = new ArrayBlockingQueue<Point>(size);
        this.waitCloseTimeLimit = waitCloseTimeLimit;
        this.backpressure = backpressure;
    }

    public void send(Point point) {
        if (forbiddenWrite.get()) {
            throw new IllegalStateException("client has been closed.");
        }
        
        if(this.backpressure){
            try {
                pointQueue.put(point);
            } catch (InterruptedException e) {
                LOGGER.error("Client Thread been Interrupted.",e);
                return;
            }
        } else {
            try {
                pointQueue.add(point);
            } catch(IllegalStateException exception) {
                throw new BufferQueueFullException("The buffer queue is full.",exception);
            }
        }
    }

    public Point receive() throws InterruptedException {
        Point point = null;
        point = pointQueue.take();
        return point;
    }

    public Point receive(int timeout) throws InterruptedException {
        Point point = pointQueue.poll(timeout, TimeUnit.MILLISECONDS);
        return point;
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