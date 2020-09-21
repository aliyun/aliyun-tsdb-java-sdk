package com.aliyun.hitsdb.client.value.response.batch;

import com.aliyun.hitsdb.client.value.Result;

import java.util.List;

/**
 * Copyright @ 2020 alibaba.com
 * All right reserved.
 * Function：IgnoreErrors Result
 *
 * @author Benedict Jin
 * @since 2020/09/21
 */
public class IgnoreErrorsResult extends Result {

    private List<ErrorPoint> ignoredErrors;
    private int failed;
    private int success;

    public IgnoreErrorsResult() {
        super();
    }

    public IgnoreErrorsResult(int success, int failed, List<ErrorPoint> ignoredErrors) {
        super();
        this.success = success;
        this.failed = failed;
        this.ignoredErrors = ignoredErrors;
    }

    public List<ErrorPoint> getIgnoredErrors() {
        return ignoredErrors;
    }

    public int getFailed() {
        return failed;
    }

    public int getSuccess() {
        return success;
    }

    public void setIgnoredErrors(List<ErrorPoint> ignoredErrors) {
        this.ignoredErrors = ignoredErrors;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public void setSuccess(int success) {
        this.success = success;
    }
}
