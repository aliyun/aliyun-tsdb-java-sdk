package com.aliyun.hitsdb.client.callback;

public abstract class AbstractCallback<REQ, RES> implements Callback<REQ, RES> {

    @Override
    public void failed(String address, REQ request, Exception ex) {
    }
}
