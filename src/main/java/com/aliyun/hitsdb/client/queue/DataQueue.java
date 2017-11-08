package com.aliyun.hitsdb.client.queue;

import com.aliyun.hitsdb.client.value.request.Point;

public interface DataQueue {
    /**
     * 发送
     * 
     * @param point
     */
    void send(Point point);

    /**
     * 获取Point，获取不到则线程阻塞。
     * 
     * @return
     */
    Point receive() throws InterruptedException;

    /**
     * 获取Point，若超时返回null。
     * 
     * @param timeout
     *            超时时间，单位毫秒
     * @return
     */
    Point receive(int timeout) throws InterruptedException;

    /**
     * 禁止写入数据
     */
    void forbiddenSend();

    /**
     * 等待队列为空
     */
    void waitEmpty();
    
    /**
     * 是否为空
     * @return
     */
    boolean isEmpty();
}
