package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author johnnyzou
 */
public class TagsAddInfo extends JSONValue {
    private String metric;
    private Map<String, String> tags;
    private List<String> _ids;

    public TagsAddInfo(String metric, Map<String, String> tags, List<String> _ids) {
        this.metric = metric;
        this.tags = tags;
        this._ids = _ids;
    }

    public static class Builder {
        private String metric;
        private Map<String, String> tags = new HashMap();
        private List<String> _ids = new ArrayList();

        public Builder(String metric) {
            this.metric = metric;
        }

        public Builder tag(String tagk, String tagv) {
            this.tags.put(tagk, tagv);
            return this;
        }

        public Builder id(String id) {
            this._ids.add(id);
            return this;
        }

        public Builder ids(List<String> ids) {
            this._ids.addAll(ids);
            return this;
        }

        public TagsAddInfo build() {
            if (tags.size() == 0) {
                throw new IllegalArgumentException("missing tags.");
            }
            if (this._ids.size() == 0) {
                throw new IllegalArgumentException("missing ids.");
            }
            return new TagsAddInfo(this.metric, this.tags, this._ids);
        }
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<String> get_ids() {
        return _ids;
    }
}
