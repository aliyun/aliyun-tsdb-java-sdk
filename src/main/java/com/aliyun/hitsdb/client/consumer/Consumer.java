package com.aliyun.hitsdb.client.consumer;

public interface Consumer {
    void start();
    void stop();
    void stop(boolean force);
}