package com.alibaba.hitsdb.client;

import com.alibaba.hitsdb.client.exception.VIPClientException;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;

public class HiTSDBClientFactory {
    public static HiTSDB connect(String host,int port) throws VIPClientException, HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig.address(host, port).config();
        HiTSDB client = new HiTSDBClient(config);
        return client;
    }
    
    public static HiTSDB connect(HiTSDBConfig config) throws VIPClientException, HttpClientInitException {
        HiTSDB client = new HiTSDBClient(config);
        return client;
    }
    
}
