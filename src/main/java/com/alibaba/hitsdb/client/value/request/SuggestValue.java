package com.alibaba.hitsdb.client.value.request;

import com.alibaba.hitsdb.client.value.JSONValue;

public class SuggestValue extends JSONValue {
    private String type;
    private String q;
    private int max;

    public SuggestValue() {
        super();
    }

    public SuggestValue(String type, String q, int max) {
        super();
        this.type = type;
        this.q = q;
        this.max = max;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

}
