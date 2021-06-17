package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.annotation.JSONField;
import com.aliyun.hitsdb.client.util.Pair;
import com.aliyun.hitsdb.client.value.Result;

import java.util.List;

/**
 * @author johnnyzou
 */
public class TagsAddResult extends Result {
    private List<DetailPair> details;
    @JSONField(alternateNames = "failure")
    private int failed;
    private int success;

    public TagsAddResult() {
        super();
    }

    public TagsAddResult(int success, int failed, List<DetailPair> details) {
        super();
        this.success = success;
        this.failed = failed;
        this.details = details;
    }

    public List<DetailPair> getDetails() {
        return details;
    }

    public int getFailed() {
        return failed;
    }

    public int getSuccess() {
        return success;
    }

    public void setDetails(List<DetailPair> details) {
        this.details = details;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public void setSuccess(int success) {
        this.success = success;
    }

    public static class DetailPair<K, V> {
        protected K id;
        protected V reason;

        public DetailPair(final K id, final V reason) {
            this.id = id;
            this.reason = reason;
        }
        public K getId() {
            return id;
        }

        public void setId(K id) {
            this.id = id;
        }
        public V getReason() {
            return reason;
        }

        public void setReason(V reason) {
            this.reason = reason;
        }
    }
}