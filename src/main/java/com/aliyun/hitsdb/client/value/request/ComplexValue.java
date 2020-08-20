package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSONObject;

import java.util.Map;

/**
 * @author cuiyuan
 * @date 2020/8/10 11:53 上午
 */

public class ComplexValue {
    public static final String TypeKey = "type";
    public static final String ContentKey = "content";

    String type;
    String content;

    public ComplexValue(String type, String content) {
        this.type = type;
        this.content = content;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public static boolean isJsonObjectTypeMatch(JSONObject jsonObject) {
        return jsonObject.containsKey(TypeKey) && jsonObject.containsKey(ContentKey) && jsonObject.size() == 2;
    }
}