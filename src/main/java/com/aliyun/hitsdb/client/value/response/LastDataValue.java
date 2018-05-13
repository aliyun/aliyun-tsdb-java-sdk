package com.aliyun.hitsdb.client.value.response;

import java.util.Map;

public class LastDataValue {
    private String metric;
    private long timestamp;
    private Object value;
    private String tsuid;
    private Map<String, String> tags;

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String getTsuid() {
        return tsuid;
    }

    public void setTsuid(String tsuid) {
        this.tsuid = tsuid;
    }

    @Override
    public String toString() {
        return "LastDPValue [metric=" + metric + ", timestamp=" + timestamp + ", value=" + value + ", tags=" + tags
                + ", tsuid=" + tsuid + "]";
    }

}
