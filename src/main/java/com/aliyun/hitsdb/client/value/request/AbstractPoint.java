package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.annotation.JSONType;
import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.Map;

@JSONType(ignores = {"pointType"})
public abstract class AbstractPoint extends JSONValue {

    protected String metric;
    protected Map<String, String> tags;
    protected Long timestamp;

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public abstract PointType getPointType();

    /**
     * If it is true, it is a legitimate character.
     * @param c char
     * @return
     */
    public static boolean checkChar(char c) {
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') ||
                c == '-' || c == '_' ||
                c == '.' || c == ' ' || c == ',' || c == '=' || c == '/' || c == ':' ||
                c == '(' || c == ')' || c == '[' || c == ']' || c == '\'' || c == '/' || c == '#' ||
                Character.isLetter(c);
    }


    private static final long MIN_TIME = 4284768L;

    private static final long MAX_TIME = 9999999999999L;

    public static void checkTimestamp(long timestamp) {
        if (timestamp < MIN_TIME || timestamp > MAX_TIME) {
            throw new IllegalArgumentException("The timestamp must be in range [4284768,9999999999999],but is " + timestamp);
        }
    }
}
