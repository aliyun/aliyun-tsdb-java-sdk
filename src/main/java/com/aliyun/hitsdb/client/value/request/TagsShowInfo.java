package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

/**
 * @author johnnyzou
 */
public class TagsShowInfo extends JSONValue {
    private String metric;
    private String _id;

    public TagsShowInfo(String metric, String _id) {
        this.metric = metric;
        this._id = _id;
    }
    public String getMetric() {
        return metric;
    }

    public String get_id() {
        return _id;
    }
}
