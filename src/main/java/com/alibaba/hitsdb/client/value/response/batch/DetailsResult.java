package com.alibaba.hitsdb.client.value.response.batch;

import java.util.List;

import com.alibaba.hitsdb.client.value.Result;

public class DetailsResult extends Result {
    private List<ErrorPoint> errors;
    private int failed;
    private int success;

    public DetailsResult() {
        super();
    }

    public DetailsResult(int success, int failed, List<ErrorPoint> errors) {
        super();
        this.success = success;
        this.failed = failed;
        this.errors = errors;
    }

    public List<ErrorPoint> getErrors() {
        return errors;
    }

    public int getFailed() {
        return failed;
    }

    public int getSuccess() {
        return success;
    }

    public void setErrors(List<ErrorPoint> errors) {
        this.errors = errors;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public void setSuccess(int success) {
        this.success = success;
    }

}
