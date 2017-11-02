package com.alibaba.hitsdb.client.value.response.batch;

import com.alibaba.hitsdb.client.value.request.Point;

public class ErrorPoint {
    private Point datapoint;
    private String error;

    public ErrorPoint() {
        super();
    }

    public ErrorPoint(Point datapoint, String error) {
        super();
        this.datapoint = datapoint;
        this.error = error;
    }

    public Point getDatapoint() {
        return datapoint;
    }

    public String getError() {
        return error;
    }

    public void setDatapoint(Point datapoint) {
        this.datapoint = datapoint;
    }

    public void setError(String error) {
        this.error = error;
    }

}
