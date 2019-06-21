package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

public class SQLValue extends JSONValue {
    private String sql;

    public SQLValue(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
