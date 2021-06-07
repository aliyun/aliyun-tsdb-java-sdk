package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.HashMap;
import java.util.Map;

/**
 * @author johnnyzou
 */
public class TagsRemoveInfo extends JSONValue{
    private String metric;
    private Map<String, String> tags;
    private String _id;

    public TagsRemoveInfo(String metric, Map<String, String> tags, String _id) {
        this.metric = metric;
        this.tags = tags;
        this._id = _id;
    }

    public static class Builder {
        private String metric;
        private Map<String, String> tags = new HashMap();
        private String _id;

        public Builder(String metric, String _id) {
            this.metric = metric;
            this._id = _id;
        }

        public Builder tag(String tagk, String tagv) {
            this.tags.put(tagk, tagv);
            return this;
        }

        public TagsRemoveInfo build() {
            return new TagsRemoveInfo(this.metric, this.tags, this._id);
        }
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String get_id() {
        return _id;
    }

}
