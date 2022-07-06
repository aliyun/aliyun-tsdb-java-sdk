package com.aliyun.hitsdb.client.value.request;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created By jianhong.hjh
 * Date: 2018/10/29
 */
public class LastPointSubQuery extends HashMap<String, Object> {

    public static class Builder {
        private LastPointSubQuery query = new LastPointSubQuery();

        private LastLimit limit;

        private Integer rlimit;

        private Integer roffset;

        private Integer slimit;

        public Builder(String metric) {
            this.query.setMetric(metric);
        }
    
        public Builder(String metric, Map<String, String> tags) {
            this.query.setMetric(metric);
            this.query.setTags(tags);
        }

        public Builder(String metric, List<String> fields, Map<String, String> tags) {
            this.query.setMetric(metric);
            this.query.setTags(tags);
            this.query.setFields(fields);
        }

        public Builder(List<String> tsuids) {
            this.query.setTsuids(tsuids);
        }

        public Builder hint(Map<String, Map<String, Integer>> hint) {
            this.query.setHint(hint);
            return this;
        }

        public Builder limit(LastLimit limit) {
            this.query.setLimit(limit);
            return this;
        }

        /**
         * only support for multi field model
         */
        public Builder rlimit(Integer rlimit) {
            this.query.setRlimit(rlimit);
            return this;
        }

        /**
         * only support for multi field model
         */
        public Builder roffset(Integer roffset) {
            this.query.setRoffset(roffset);
            return this;
        }

        /**
         * current only support for super tag model
         */
        public Builder slimit(Integer slimit) {
            this.query.setSlimit(slimit);
            return this;
        }

        public LastPointSubQuery build() {
            // prevent an empty hint map
            if ((this.query.getHint() != null) && (this.query.getHint().isEmpty())) {
                throw new IllegalArgumentException("cannot specify an empty hint");
            }
            return this.query;
        }
    }

    public static Builder builder(String metric) {
        return new Builder(metric);
    }

    public static Builder builder(String metric, Map<String, String> tags) {
        return new Builder(metric, tags);
    }

    public static Builder builder(String metric, List<String> fields, Map<String, String> tags) {
        return new Builder(metric, fields, tags);
    }

    public static Builder builder(List<String> tsuids){
        return new Builder(tsuids);
    }

    private static final String METRIC = "metric";
    /**
     * Optional fields input parameter.
     * Used for fields' latest data points under certain metric.
     */
    private static final String FIELDS = "fields";
    private static final String TAGS = "tags";
    private static final String TSUIDS = "tsuids";
    private static final String HINT   = "hint";
    private static final String LIMIT   = "limit";
    private static final String RLIMIT   = "rlimit";
    private static final String ROFFSET   = "roffset";
    private static final String SLIMIT   = "slimit";

    public String getMetric() {
        return (String) this.get(METRIC);
    }

    public void setMetric(String metric) {
        if (getTsuids() != null) {
            throw new IllegalArgumentException("metric and tsuid parameter provided at the same time is not supported.");
        }
        this.put(METRIC, metric);
    }

    public List<String> getFields() {
        return (List<String>) this.get(FIELDS);
    }

    public void setFields(List<String> fields) {
        this.put(FIELDS, fields);
    }

    public Map<String, String> getTags() {
        return (Map<String, String>) this.get(TAGS);
    }

    public void setTags(Map<String, String> tags) {
        this.put(TAGS, tags);
    }

    public List<String> getTsuids() {
        return (List<String>) this.get(TSUIDS);
    }

    public void setTsuids(List<String> tsuids) {
        if (getMetric() != null) {
            throw new IllegalArgumentException("metric and tsuid parameter provided at the same time is not supported.");
        }
        this.put(TSUIDS, tsuids);
    }

    public Map<String, Map<String, Integer>> getHint() {
        return (Map<String, Map<String, Integer>>)this.get(HINT);
    }

    public void setHint(Map<String, Map<String, Integer>> hint) {
        this.put(HINT, hint);
    }

    public void setLimit(LastLimit limit) {
        this.put(LIMIT, limit);
    }

    public void setRlimit(Integer rlimit) {
        this.put(RLIMIT, rlimit);
    }

    public void setRoffset(Integer roffset) {
        this.put(ROFFSET, roffset);
    }

    public void setSlimit(Integer slimit) {
        this.put(SLIMIT, slimit);
    }
}
