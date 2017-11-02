package com.alibaba.hitsdb.client.callback;

public interface Callback<REQ, RES> {
    void response(String address, REQ input, RES output);
    
    void failed(String address, REQ input,Exception ex);
}