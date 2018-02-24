package com.aliyun.hitsdb.client.util;

public class TSDBNetAddress {
    private String host;
    private int port;

    public TSDBNetAddress(String host, int port) {
        super();
        
        if(host == null || host.isEmpty()) {
            throw new IllegalArgumentException("the host can't be null.");
        }
        
        if(port <= 0) {
            throw new IllegalArgumentException("the port are illegal.");
        }
        
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

}
