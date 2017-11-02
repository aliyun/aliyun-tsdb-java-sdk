package com.alibaba.hitsdb.client.value.request;

import com.alibaba.hitsdb.client.value.JSONValue;

public class MetricTimeRange extends JSONValue {
    private String metric;
    private int start;
    private int end;

    public MetricTimeRange() {
        super();
    }

    public MetricTimeRange(String metric, int start, int end) {
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

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

}
