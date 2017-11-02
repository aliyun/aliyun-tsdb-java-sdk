package com.alibaba.hitsdb.client.value.response;

import com.alibaba.hitsdb.client.value.JSONValue;

public class TTLResult extends JSONValue {
    private int val;

    public TTLResult() {
        super();
    }

    public TTLResult(int val) {
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
