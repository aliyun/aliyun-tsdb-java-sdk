package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.annotation.JSONField;
import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MultiFieldQuery extends JSONValue {

    private Long start;
    private Long end;
    private List<MultiFieldSubQuery> queries;
    private Boolean msResolution;
    @JSONField(serialize = false)
    private boolean showType;
    @JSONField(serialize = false)
    private List<Class<?>> types;
    private Map<String, Map<String, Integer>> hint;

    public static class Builder {
        private Long startTime;
        private Long endTime;
        private Boolean msResolution;
        private List<MultiFieldSubQuery> subQueryList = new ArrayList<MultiFieldSubQuery>();
        private boolean showType;
        private List<Class<?>> types;
        private Map<String, Map<String, Integer>> hint;

        /**
         * 1970-02-20 00:59:28
         */
        private static final long MIN_START_TIME = 4294968;
        /**
         * 2286-11-21 01:46:39.999
         */
        private static final long MAX_END_TIME = 9999999999999L;

        public Builder(long startTime) {
            this.startTime = startTime;
        }

        public Builder(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public Builder end(Date endDate) {
            this.endTime = endDate.getTime();
            return this;
        }

        public Builder end(long endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder msResolution(Boolean msResolution) {
            if (msResolution) {
                this.msResolution = true;
            } else {
                this.msResolution = null;
            }
            return this;
        }

        public Builder msResolution() {
            this.msResolution = true;
            return this;
        }

        /**
         * After calling this method, the data type of the data point will be displayed in the query result.
         */
        public Builder showType() {
            this.showType = true;
            return this;
        }

        /**
         * The user can specify the data type of data points under different fields in the query results.
         */
        public Builder withTypes(List<Class<?>> types) {
            this.showType = true;
            this.types = types;
            return this;
        }

        public Builder hint(Map<String, Map<String, Integer>> hint) {
            this.hint = hint;
            return this;
        }

        public Builder sub(MultiFieldSubQuery... subQuerys) {
            for (MultiFieldSubQuery subQuery : subQuerys) {
                subQueryList.add(subQuery);
            }
            return this;
        }

        public Builder sub(Collection<MultiFieldSubQuery> subQuerys) {
            for (MultiFieldSubQuery subQuery : subQuerys) {
                subQueryList.add(subQuery);
            }
            return this;
        }

        public MultiFieldQuery build() {
            MultiFieldQuery query = new MultiFieldQuery();
            query.queries = this.subQueryList;
            if (this.startTime == null) {
                throw new IllegalArgumentException("the start time must be set");
            }
            if (this.startTime < MIN_START_TIME) {
                throw new IllegalArgumentException("the start time must be greater than " + MIN_START_TIME);
            }
            query.start = this.startTime;

            if (this.endTime != null) {
                if(this.endTime > MAX_END_TIME) {
                    throw new IllegalArgumentException("the end time must be less than" + MAX_END_TIME);
                }
                if(this.endTime < this.startTime) {
                    throw new IllegalArgumentException("the end time (" + this.endTime +
                            ") must be greater than start time (" + this.startTime + ")" );
                }
            }
            query.end = this.endTime;
            if (this.subQueryList == null || this.subQueryList.isEmpty()) {
                throw new IllegalArgumentException("Missing sub queries.");
            }
            query.queries = this.subQueryList;
            query.msResolution = this.msResolution;
            query.showType = this.showType;
            query.types = this.types;
            query.hint = this.hint;
            return query;
        }
    }

    /**
     * set the start time
     * @param startTime start timestamp
     * @return Builder get a builder
     */
    public static Builder start(long startTime) {
        return new Builder(startTime);
    }

    /**
     * set the start date
     * @param startDate start date
     * @return Builder
     */
    public static Builder start(Date startDate) {
        long startTime = startDate.getTime();
        return new Builder(startTime);
    }

    /**
     * set the start date and the end date
     * @param startDate start date
     * @param endDate end date
     * @return Builder
     */
    public static Builder timeRange(Date startDate, Date endDate) {
        long startTime = startDate.getTime();
        long endTime = endDate.getTime();
        return new Builder(startTime, endTime);
    }

    /**
     * set the start time and the end time
     * @param startTime start timestamp
     * @param endTime end timestamp
     * @return Builder
     */
    public static Builder timeRange(long startTime, long endTime) {
        return new Builder(startTime, endTime);
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public Boolean getMsResolution() {
        return msResolution;
    }

    public List<MultiFieldSubQuery> getQueries() {
        return queries;
    }

    public boolean isShowType() {
        return showType;
    }

    public void setShowType(boolean showType) {
        this.showType = showType;
    }

    public List<Class<?>> getTypes() {
        return types;
    }

    public void setTypes(List<Class<?>> types) {
        this.types = types;
    }

    public Map<String, Map<String, Integer>> getHint() {
        return hint;
    }

    public void setHint(Map<String, Map<String, Integer>> hint) {
        this.hint = hint;
    }
}
