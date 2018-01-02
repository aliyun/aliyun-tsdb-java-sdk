package com.aliyun.hitsdb.client.queue;

import com.aliyun.hitsdb.client.value.request.Point;

public interface DataQueue {
    void send(Point point);

    Point receive() throws InterruptedException;

    Point receive(int timeout) throws InterruptedException;

    void forbiddenSend();

    void waitEmpty();
    
    boolean isEmpty();
}
