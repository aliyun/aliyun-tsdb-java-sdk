package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;

public class TSDBClientFactory {

    public static TSDB connect(String host, int port) throws HttpClientInitException {
        TSDBConfig config = TSDBConfig.address(host, port).config();
        TSDB client = new TSDBClient(config);
        return client;
    }

    public static TSDB connect(Config config) throws HttpClientInitException {
        TSDB client = new TSDBClient(config);
        return client;
    }

}
