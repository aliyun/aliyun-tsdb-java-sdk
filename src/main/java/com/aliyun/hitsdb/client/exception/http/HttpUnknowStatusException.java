package com.aliyun.hitsdb.client.exception.http;

import com.aliyun.hitsdb.client.http.response.ResultResponse;

public class HttpUnknowStatusException extends RuntimeException {

    private static final long serialVersionUID = 3183070191347274019L;

    public HttpUnknowStatusException(int status, String message) {
        super();
        this.status = status;
        this.message = message;
    }

    public HttpUnknowStatusException(ResultResponse result) {
        super();
        this.status = result.getStatusCode();
        this.message = result.getContent();
    }

    private int status;

    private String message;

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

}
