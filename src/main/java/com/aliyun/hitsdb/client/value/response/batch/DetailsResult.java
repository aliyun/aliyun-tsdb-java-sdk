package com.aliyun.hitsdb.client.value.response.batch;

import java.util.List;

import com.aliyun.hitsdb.client.value.Result;

public class DetailsResult extends SummaryResult {
    private List<ErrorPoint> errors;

    public DetailsResult() { super(); }

    public DetailsResult(int success, int failed, List<ErrorPoint> errors) {
        super(success, failed);
        this.errors = errors;
    }

    public List<ErrorPoint> getErrors() {
        return errors;
    }

    public void setErrors(List<ErrorPoint> errors) {
        this.errors = errors;
    }

}
