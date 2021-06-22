package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;

/**
 * @author johnnyzou
 * @description create client for IoT(Super-tag mode) user
 */
public class IotClientFactory {
    public static TSDB connect(String host, int port) throws HttpClientInitException {
        TSDBConfig config = TSDBConfig.address(host, port).config();
        TSDB client = new IotClient(config);
        return client;
    }

    public static TSDB connect(Config config) throws HttpClientInitException {
        TSDB client = new IotClient(config);
        return client;
    }
}
