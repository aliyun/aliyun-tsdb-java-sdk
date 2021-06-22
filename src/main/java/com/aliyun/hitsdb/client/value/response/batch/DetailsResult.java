package com.aliyun.hitsdb.client.value.response.batch;

import java.util.List;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;

public class DetailsResult extends Result {
    private List<ErrorPoint> errors;
    private int failed;
    private int success;
    /**
     * fatal is only used for Lindorm TSDB
     */
    private List<Point> fatal;

    public DetailsResult() {
        super();
    }

    public DetailsResult(int success, int failed, List<ErrorPoint> errors, List<Point> fatal) {
        super();
        this.success = success;
        this.failed = failed;
        this.errors = errors;
        this.fatal = fatal;
    }

    public List<Point> getFatal() {
        return fatal;
    }

    public void setFatal(List<Point> fatal) {
        this.fatal = fatal;
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
