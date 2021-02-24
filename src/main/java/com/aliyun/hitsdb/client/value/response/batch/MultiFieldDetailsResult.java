package com.aliyun.hitsdb.client.value.response.batch;

import com.aliyun.hitsdb.client.value.Result;

import java.util.List;

public class MultiFieldDetailsResult extends SummaryResult {
    private List<MultiFieldErrorPoint> errors;

    public MultiFieldDetailsResult() {
        super();
    }

    public MultiFieldDetailsResult(int success, int failed, List<MultiFieldErrorPoint> errors) {
        super(success, failed);
        this.errors = errors;
    }

    public List<MultiFieldErrorPoint> getErrors() {
        return errors;
    }

    public void setErrors(List<MultiFieldErrorPoint> errors) {
        this.errors = errors;
    }
}
