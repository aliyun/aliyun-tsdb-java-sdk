package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

public class DumpMetaValue extends JSONValue {
    private String metric;
    private String tagkey;
    private String tagvalueprefix;
    private int max;

    private boolean dump_metric;

    public DumpMetaValue() {
        super();
    }

    public DumpMetaValue(String tagkey, String tagvalueprefix, int max) {
        super();
        this.tagkey = tagkey;
        this.tagvalueprefix = tagvalueprefix;
        this.max = max;
        this.dump_metric = false;
    }

    public DumpMetaValue(String tagkey, String tagvalueprefix, int max,boolean dumpMetric) {
        super();
        this.tagkey = tagkey;
        this.tagvalueprefix = tagvalueprefix;
        this.max = max;
        this.dump_metric = dumpMetric;
    }

    public DumpMetaValue(String metric,String tagkey, String tagvalueprefix, int max) {
        super();
        this.metric = metric;
        this.tagkey = tagkey;
        this.tagvalueprefix = tagvalueprefix;
        this.max = max;
        this.dump_metric = false;
    }


    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public boolean isDump_metric() {
        return dump_metric;
    }

    public void setDump_metric(boolean dump_metric) {
        this.dump_metric = dump_metric;
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
