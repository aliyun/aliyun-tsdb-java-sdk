package com.aliyun.hitsdb.client.value.response;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class LastDataValue {
    private String metric;
    private String field;
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

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
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

    public String tagsToString() {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        Set<String> tagks = new TreeSet<String>();
        tagks.addAll(tags.keySet());
        StringBuilder tagsString = new StringBuilder();
        boolean firstTag = true;
        for (String tagk : tagks) {
            if (firstTag) {
                tagsString.append(tagk).append("$").append(tags.get(tagk));
                firstTag = false;
            } else {
                tagsString.append("$").append(tagk).append("$").append(tags.get(tagk));
            }
        }
        return tagsString.toString();
    }
}
