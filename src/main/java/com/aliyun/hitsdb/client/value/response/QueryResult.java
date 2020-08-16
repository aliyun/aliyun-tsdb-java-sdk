package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.annotation.JSONField;
import com.aliyun.hitsdb.client.value.JSONValue;
import com.aliyun.hitsdb.client.value.request.ByteArrayValue;

import java.util.*;

public class QueryResult extends JSONValue {
    private String metric;
    private Map<String, String> tags;
    private List<String> aggregateTags;
    @JSONField(serializeUsing = QueryResultDpsSerializer.class, deserializeUsing = QueryResultDpsSerializer.class)
    private LinkedHashMap<Long, Object> dps = new LinkedHashMap<Long, Object>();
    private Class<?> type;
    @Deprecated
    private LinkedHashMap<Long, String> sdps = new LinkedHashMap<Long, String>();

    private static final Comparator<KeyValue> ORDER_CMP = new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue keyValue, KeyValue t1) {
           long diff = keyValue.getTimestamp() - t1.getTimestamp();
           return diff == 0 ? 0 : (diff > 0 ? 1 : -1);
        }
    };

    public static final Comparator<KeyValue> REVERSE_ORDER_CMP = new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue keyValue, KeyValue t1) {
            long diff = keyValue.getTimestamp() - t1.getTimestamp();
            return diff == 0 ? 0 : (diff > 0 ? -1 : 1);
        }
    };

    public List<KeyValue> getOrderDps() {
       return getOrderDps(false);
    }

    public List<KeyValue> getOrderDps(boolean reverse) {
        if(dps == null || dps.isEmpty()) {
            return Collections.emptyList();
        }
        List<KeyValue> keyValues = new ArrayList<KeyValue>(dps.size());
        for(Map.Entry<Long,Object> entry : dps.entrySet()) {
            if (entry.getValue() instanceof byte[]){
                keyValues.add(new KeyValue(entry.getKey(), new ByteArrayValue((byte [])entry.getValue())));
            }else{
                keyValues.add(new KeyValue(entry.getKey(),entry.getValue()));
            }
        }
        if(reverse) {
            Collections.sort(keyValues,REVERSE_ORDER_CMP);
        } else {
            Collections.sort(keyValues,ORDER_CMP);
        }
        return keyValues;
    }
    
    public List<String> getAggregateTags() {
        return aggregateTags;
    }

    public void setAggregateTags(List<String> aggregateTags) {
        this.aggregateTags = aggregateTags;
    }

    public LinkedHashMap<Long, Object> getDps() {
        return dps;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setDps(LinkedHashMap<Long, Object> dps) {
        this.dps = dps;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    @Deprecated
    public LinkedHashMap<Long, String> getSdps() {
        return sdps;
    }

    @Deprecated
    public void setSdps(LinkedHashMap<Long, String> sdps) {
        this.sdps = sdps;
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
