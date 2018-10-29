package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Created By jianhong.hjh
 * Date: 2018/10/29
 */
public class LastPointQuery extends JSONValue {


    public static class Builder {
        private int backScan = 0;

        private boolean msResolution;

        private long timestamp;

        private List<LastPointSubQuery> queries;

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

        public Builder sub(LastPointSubQuery subQuery) {
            if (queries == null) {
                queries = new ArrayList<LastPointSubQuery>();
            }
            queries.add(subQuery);
            return this;
        }

        public LastPointQuery build() {
            LastPointQuery query = new LastPointQuery();
            query.setBackScan(this.backScan);
            query.setMsResolution(msResolution);
            query.setTimestamp(timestamp);
            query.setQueries(queries);
            return query;
        }
    }


    public static Builder builder(){
        return new Builder();
    }

    private boolean msResolution;

    private int backScan;

    private long timestamp;


    private List<LastPointSubQuery> queries;

    public boolean isMsResolution() {
        return msResolution;
    }

    public void setMsResolution(boolean msResolution) {
        this.msResolution = msResolution;
    }

    public int getBackScan() {
        return backScan;
    }

    public void setBackScan(int backScan) {
        this.backScan = backScan;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<LastPointSubQuery> getQueries() {
        return queries;
    }

    public void setQueries(List<LastPointSubQuery> queries) {
        this.queries = queries;
    }
}
