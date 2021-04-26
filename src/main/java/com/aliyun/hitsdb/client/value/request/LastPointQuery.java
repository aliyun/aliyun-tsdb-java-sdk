package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.annotation.JSONField;
import com.aliyun.hitsdb.client.value.JSONValue;
import com.aliyun.hitsdb.client.value.type.QueryType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created By jianhong.hjh
 * Date: 2018/10/29
 */
public class LastPointQuery extends JSONValue {


    public static class Builder {
        private Integer backScan;

        private Boolean msResolution;

        private Long timestamp;

        private List<LastPointSubQuery> queries;

        private Boolean tupleFormat;

        private LastLimit limit;

        private Integer rlimit;

        private Integer roffset;

        private Integer slimit;

        private Map<String, Map<String, Integer>> hint;

        // this field is only allowed for Lindorm TSDB
        private String queryType;

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder backScan(int backScan) {
            this.backScan = backScan;
            return this;
        }

        public Builder msResolution(boolean msResolution) {
            this.msResolution = msResolution;
            return this;
        }

        public Builder hint(Map<String, Map<String, Integer>> hint) {
            this.hint = hint;
            return this;
        }

        public Builder sub(LastPointSubQuery subQuery) {
            if (queries == null) {
                queries = new ArrayList<LastPointSubQuery>();
            }
            queries.add(subQuery);
            return this;
        }

        public Builder tupleFormat(boolean tupleFormat) {
            this.tupleFormat = tupleFormat;
            return this;
        }

        public Builder limit(LastLimit limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Set queryType is only allowed for Lindorm TSDB.
         * Currently only support a few data type.
         */
        public Builder queryType(QueryType queryType) {
            this.queryType = queryType.getName();
            return this;
        }

        /**
         * only support for multi field model
         */
        public Builder rlimit(Integer rlimit) {
            this.rlimit = rlimit;
            return this;
        }

        /**
         * only support for multi field model
         */
        public Builder roffset(Integer roffset) {
            this.roffset = roffset;
            return this;
        }

        /**
         * current only support for super tag model
         */
        public Builder slimit(Integer slimit) {
            this.slimit = slimit;
            return this;
        }

        public LastPointQuery build() {
            if (queries == null || queries.isEmpty()) {
                throw new IllegalArgumentException("the LastPointSubQuery must be set");
            }
            LastPointQuery query = new LastPointQuery();
            query.backScan = this.backScan;
            query.msResolution = msResolution;
            query.timestamp = timestamp;
            query.setQueries(queries);
            query.tupleFormat = tupleFormat;
            query.limit = limit;
            query.rlimit = rlimit;
            query.roffset = roffset;
            query.slimit = slimit;
            query.hint = hint;
            query.queryType = queryType;
            return query;
        }
    }


    public static Builder builder() {
        return new Builder();
    }

    private Boolean msResolution;

    private Integer backScan;

    private Long timestamp;

    private LastLimit limit;

    private Integer rlimit;

    private Integer roffset;

    private Integer slimit;

    /**
     * Optional tupleFormat parameter.
     * Used for multi-field latest data points query and show results in tuple format.
     */
    private Boolean tupleFormat;

    private List<LastPointSubQuery> queries;

    private Map<String, Map<String, Integer>> hint;

    // this field is only allowed for Lindorm TSDB
    @JSONField(name = "type")
    private String queryType;

    public Boolean isMsResolution() {
        return msResolution;
    }

    public void setMsResolution(boolean msResolution) {
        this.msResolution = msResolution;
    }

    public Integer getBackScan() {
        return backScan;
    }

    public void setBackScan(int backScan) {
        this.backScan = backScan;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Boolean getTupleFormat() {
        return tupleFormat;
    }

    public void setTupleFormat(Boolean tupleFormat) {
        this.tupleFormat = tupleFormat;
    }

    public List<LastPointSubQuery> getQueries() {
        return queries;
    }

    public void setQueries(List<LastPointSubQuery> queries) {
        this.queries = queries;
    }

    public LastLimit getLimit() {
        return limit;
    }

    public void setLimit(LastLimit limit) {
        this.limit = limit;
    }

    public Integer getRlimit() {
        return rlimit;
    }

    public void setRLimit(int rlimit) {
        this.rlimit = rlimit;
    }

    public Integer getRoffset() {
        return roffset;
    }

    public void setRoffset(int roffset) {
        this.roffset = roffset;
    }

    public Integer getSlimit() {
        return slimit;
    }

    public void setSLimit(int slimit) {
        this.slimit = slimit;
    }

    public Map<String, Map<String, Integer>> getHint() {
        return hint;
    }

    public void setHint(Map<String, Map<String, Integer>> hint) {
        this.hint = hint;
    }

    public String getQueryType() {
        return queryType;
    }

    /**
     * Set queryType is only allowed for Lindorm TSDB.
     * Currently only support a few data type.
     */
    public void setQueryType(QueryType queryType) {
        this.queryType = queryType.getName();
    }
}
