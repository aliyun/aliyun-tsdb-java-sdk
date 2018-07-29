package com.aliyun.hitsdb.client.util;

public interface HealthWatcher {


    void health(String host, boolean health);

}
