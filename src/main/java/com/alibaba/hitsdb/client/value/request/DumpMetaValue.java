package com.alibaba.hitsdb.client.value.request;

import com.alibaba.hitsdb.client.value.JSONValue;

public class DumpMetaValue extends JSONValue {
    private String tagkey;
    private String tagvalueprefix;
    private int max;

    public DumpMetaValue() {
        super();
    }

    public DumpMetaValue(String tagkey, String tagvalueprefix, int max) {
        super();
        this.tagkey = tagkey;
        this.tagvalueprefix = tagvalueprefix;
        this.max = max;
    }

    public String getTagkey() {
        return tagkey;
    }

    public void setTagkey(String tagkey) {
        this.tagkey = tagkey;
    }

    public String getTagvalueprefix() {
        return tagvalueprefix;
    }

    public void setTagvalueprefix(String tagvalueprefix) {
        this.tagvalueprefix = tagvalueprefix;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

}
