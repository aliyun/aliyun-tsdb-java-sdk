package com.aliyun.hitsdb.client.value.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.aliyun.hitsdb.client.value.JSONValue;

public class Query extends JSONValue {
    private long start;
    private long end;
    private List<SubQuery> queries;

    public static class Builder {
        private long startTime;
        private long endTime;
        private List<SubQuery> subQueryList = new ArrayList<SubQuery>();

        public Builder(long startTime) {
            this.startTime = startTime;
        }

        public Builder(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public Builder end(long endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder sub(SubQuery... subQuerys) {
            int newIndex = subQueryList.size();
            int i = 0;
            for (SubQuery subQuery : subQuerys) {
                if(subQuery.getIndex() <= 0) {
                    subQuery.setIndex(newIndex + i);
                }
                subQuery.setIndex(newIndex + i);
                subQueryList.add(subQuery);
                i++;
            }
            return this;
        }

        public Builder sub(Collection<SubQuery> subQuerys) {
            int newIndex = subQueryList.size();
            int i = 0;
            for (SubQuery subQuery : subQuerys) {
                if(subQuery.getIndex() <= 0) {
                    subQuery.setIndex(newIndex + i);
                }
                subQueryList.add(subQuery);
                i++;
            }
            return this;
        }

        public Query build() {
            Query query = new Query();
            query.start = this.startTime;
            query.end = this.endTime;
            query.queries = this.subQueryList;
            return query;
        }

    }

    public static Builder start(long startTime) {
        return new Builder(startTime);
    }

    public static Builder start(Date startDate) {
        long startTime = startDate.getTime();
        return new Builder(startTime);
    }

    public static Builder timeRange(Date startDate, Date endDate) {
        long startTime = startDate.getTime();
        long endTime = endDate.getTime();
        return new Builder(startTime, endTime);
    }

    public static Builder timeRange(long startTime, long endTime) {
        return new Builder(startTime, endTime);
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public List<SubQuery> getQueries() {
        return queries;
    }

}
