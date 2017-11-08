package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

public class MetricTimeRange extends JSONValue {
    private String metric;
    private long start;
    private long end;

    public MetricTimeRange() {
        super();
    }

    public MetricTimeRange(String metric, long start, long end) {
        super();
        this.metric = metric;
        this.start = start;
        this.end = end;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

}
