package com.aliyun.hitsdb.client.http;

public class Host {

    private String ip;

    private int port;

    public Host(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public Host() {
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
