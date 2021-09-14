package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;

public class LindormTSDBClientFactory {

    public static LindormTSDBClient connect(String host, int port) throws HttpClientInitException {
        TSDBConfig config = TSDBConfig.address(host, port).config();
        LindormTSDBClient client = new LindormTSDBClient(config);
        return client;
    }

    public static LindormTSDBClient connect(Config config) throws HttpClientInitException {
        LindormTSDBClient client = new LindormTSDBClient(config);
        return client;
    }

}
