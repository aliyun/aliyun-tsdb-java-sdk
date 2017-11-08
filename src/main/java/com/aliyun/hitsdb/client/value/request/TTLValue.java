package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

public class TTLValue extends JSONValue {
    private int val;

    public TTLValue() {
        super();
    }

    public TTLValue(int val) {
        super();
        this.val = val;
    }

    public int getVal() {
        return val;
    }

    public void setVal(int val) {
        this.val = val;
    }

}