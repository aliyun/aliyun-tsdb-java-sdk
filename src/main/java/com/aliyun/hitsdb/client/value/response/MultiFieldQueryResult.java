package com.aliyun.hitsdb.client.value.response;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MultiFieldQueryResult {
    private String metric;
    private List<String> aggregatedTags;
    private Map<String, String> tags;
    private List<String> columns = new ArrayList<String>();
    private List<List<Object>> values = new ArrayList<List<Object>>();
    private List<Class<?>> types;

    public String getMetric() {
        return metric;
    }

    public List<String> getAggregatedTags() {
        return aggregatedTags;
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

    public void setAggregatedTags(List<String> aggregatedTags) {
        this.aggregatedTags = aggregatedTags;
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

    public List<Class<?>> getTypes() {
        return types;
    }

    public void setTypes(List<Class<?>> types) {
        this.types = types;
    }

    public List<Map<String, Object>> asMap() {
        final List<Map<String, Object>> columnValueMap = new LinkedList<Map<String, Object>>();
        for (List<Object> value : values) {
            final LinkedHashMap<String, Object> kv = new LinkedHashMap<String, Object>();
            for (int j = 0; j < columns.size(); j++) {
                kv.put(columns.get(j), value.get(j));
            }
            columnValueMap.add(kv);
        }
        return columnValueMap;
    }

    @Override
    public String toString() {
        return "MultiFieldQueryResult{" +
                "metric='" + metric + '\'' +
                ", aggregatedTags=" + aggregatedTags +
                ", tags=" + tags +
                ", columns=" + columns +
                ", values=" + values +
                '}';
    }
}
