package com.alibaba.hitsdb.client.value.response.batch;

import com.alibaba.hitsdb.client.value.Result;

public class SummaryResult extends Result {
    private int failed;
    private int success;

    public SummaryResult() {
        super();
    }

    public SummaryResult(int success, int failed) {
        super();
        this.success = success;
        this.failed = failed;
    }

    public int getFailed() {
        return failed;
    }

    public int getSuccess() {
        return success;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public void setSuccess(int success) {
        this.success = success;
    }

}
