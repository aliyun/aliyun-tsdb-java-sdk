package com.aliyun.hitsdb.client.value.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.aliyun.hitsdb.client.value.JSONValue;

@Deprecated
public class MultiValuedQuery extends JSONValue {
    private long start;
    private long end;
    private List<MultiValuedSubQuery> queries;
    private Boolean msResolution;

    public static class Builder {
        private long startTime;
        private long endTime;
        private Boolean msResolution;
        private List<MultiValuedSubQuery> subQueryList = new ArrayList<MultiValuedSubQuery>();

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

        public Builder sub(MultiValuedSubQuery... subQuerys) {
            for (MultiValuedSubQuery subQuery : subQuerys) {
                subQueryList.add(subQuery);
            }
            return this;
        }

        public Builder sub(Collection<MultiValuedSubQuery> subQuerys) {
            for (MultiValuedSubQuery subQuery : subQuerys) {
                subQueryList.add(subQuery);
            }
            return this;
        }

        public MultiValuedQuery build() {
            MultiValuedQuery query = new MultiValuedQuery();
            query.queries = this.subQueryList;
            query.start = this.startTime;
            query.end = this.endTime;
            query.queries = this.subQueryList;
            query.msResolution = this.msResolution;
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

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public Boolean getMsResolution() {
        return msResolution;
    }

    public List<MultiValuedSubQuery> getQueries() {
        return queries;
    }
}
