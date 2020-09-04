package com.aliyun.hitsdb.client.value.response.batch;

import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;

public class MultiFieldErrorPoint {

    private MultiFieldPoint datapoint;
    private String error;

    public MultiFieldErrorPoint() {
        super();
    }

    public MultiFieldErrorPoint(MultiFieldPoint datapoint, String error) {
        super();
        this.datapoint = datapoint;
        this.error = error;
    }

    public MultiFieldPoint getDatapoint() {
        return datapoint;
    }

    public String getError() {
        return error;
    }

    public void setDatapoint(MultiFieldPoint datapoint) {
        this.datapoint = datapoint;
    }

    public void setError(String error) {
        this.error = error;
    }

}
