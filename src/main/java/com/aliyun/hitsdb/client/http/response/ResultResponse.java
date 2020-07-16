package com.aliyun.hitsdb.client.http.response;

import org.apache.hc.core5.http.HttpResponse;

public class ResultResponse {
    private int statusCode;
    private HttpStatus httpStatus;
    private String content;
    private HttpResponse httpResponse;
    private boolean compress;

    public ResultResponse(int statusCode) {
        super();
        this.statusCode = statusCode;
        if (statusCode >= 200 && statusCode < 300) {
            if(statusCode == 204){
                this.httpStatus = HttpStatus.ServerSuccessNoContent;
            } else {
                this.httpStatus = HttpStatus.ServerSuccess;
            }
        } else if (statusCode >= 400 && statusCode < 500) {
            if (statusCode == 401) {
                this.httpStatus = HttpStatus.ServerUnauthorized;
            } else {
                this.httpStatus = HttpStatus.ServerNotSupport;
            }
        } else if (statusCode >= 500 && statusCode < 600) {
            this.httpStatus = HttpStatus.ServerError;
        } else {
            this.httpStatus = HttpStatus.UnKnow;
        }
    }

    public boolean isSuccess() {
        if (statusCode >= 200 && statusCode < 300) {
            return true;
        }
        return false;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public ResultResponse(int status, String content) {
        super();
        this.statusCode = status;
        this.content = content;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getContent() {
        return content;
    }

    public boolean isCompress() {
        return compress;
    }

    public static ResultResponse simplify(HttpResponse httpResponse, boolean compress) {
        int statusCode = httpResponse.getCode();
        ResultResponse resultResponse = new ResultResponse(statusCode);
        resultResponse.httpResponse = httpResponse;
        resultResponse.compress = compress;
        return resultResponse;
    }

}