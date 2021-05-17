package com.aliyun.hitsdb.client.value.response.batch;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;

import java.util.List;

public class MultiFieldDetailsResult extends Result {
    private List<MultiFieldErrorPoint> errors;
    private int failed;
    private int success;
    /**
     * fatal is only used for Lindorm TSDB
     */
    private List<MultiFieldPoint> fatal;

    public MultiFieldDetailsResult() {
        super();
    }

    public MultiFieldDetailsResult(int success, int failed, List<MultiFieldErrorPoint> errors, List<MultiFieldPoint> fatal) {
        super();
        this.success = success;
        this.failed = failed;
        this.errors = errors;
        this.fatal = fatal;
    }

    public List<MultiFieldPoint> getFatal() {
        return fatal;
    }

    public void setFatal(List<MultiFieldPoint> fatal) {
        this.fatal = fatal;
    }

    public List<MultiFieldErrorPoint> getErrors() {
        return errors;
    }

    public int getFailed() {
        return failed;
    }

    public int getSuccess() {
        return success;
    }

    public void setErrors(List<MultiFieldErrorPoint> errors) {
        this.errors = errors;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public void setSuccess(int success) {
        this.success = success;
    }

}
