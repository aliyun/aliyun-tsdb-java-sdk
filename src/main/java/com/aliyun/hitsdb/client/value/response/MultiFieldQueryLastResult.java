package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiFieldQueryLastResult {
    private String metric;
    private Map<String, String> tags = new HashMap<String, String>();
    private List<String> columns = new ArrayList<String>();
    @JSONField(serializeUsing = MultiFieldQueryValuesSerializer.class, deserializeUsing = MultiFieldQueryValuesSerializer.class)
    private List<List<Object>> values = new ArrayList<List<Object>>();

    public String getMetric() {
        return metric;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<Object>> getValues() {
        return values;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setValues(List<List<Object>> values) {
        this.values = values;
    }

    public Map<String, String> getTags() {
        return tags;
    }
}
