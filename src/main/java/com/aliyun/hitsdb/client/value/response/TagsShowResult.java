package com.aliyun.hitsdb.client.value.response;

import com.aliyun.hitsdb.client.value.Result;
import java.util.Map;

/**
 * @author johnnyzou
 */
public class TagsShowResult extends Result {
    private String metric;
    private String _id;
    private Map<String, String> tags;

    public TagsShowResult(String metric, String _id, Map<String, String> tags) {
        super();
        this.metric = metric;
        this._id = _id;
        this.tags = tags;
    }

    public TagsShowResult() {
        super();
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
