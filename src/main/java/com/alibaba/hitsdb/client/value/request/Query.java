package com.alibaba.hitsdb.client.value.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.alibaba.hitsdb.client.value.JSONValue;

public class Query extends JSONValue {
    private int start;
    private int end;
    private List<SubQuery> queries;

    public static class Builder {
        private int startTime;
        private int endTime;
        private List<SubQuery> subQueryList = new ArrayList<SubQuery>();

        public Builder(int startTime) {
            this.startTime = startTime;
        }

        public Builder(int startTime, int endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public Builder end(int endTime) {
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

    public static Builder start(int startTime) {
        return new Builder(startTime);
    }

    public static Builder start(Date startDate) {
        int startTime = (int) (startDate.getTime() / 1000);
        return new Builder(startTime);
    }

    public static Builder timeRange(Date startDate, Date endDate) {
        int startTime = (int) (startDate.getTime() / 1000);
        int endTime = (int) (endDate.getTime() / 1000);
        return new Builder(startTime, endTime);
    }

    public static Builder timeRange(int startTime, int endTime) {
        return new Builder(startTime, endTime);
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public List<SubQuery> getQueries() {
        return queries;
    }

}
