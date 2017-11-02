package com.alibaba.hitsdb.client.exception.http;

import com.alibaba.hitsdb.client.http.response.ResultResponse;

public class HttpServerNotSupportException extends HttpUnknowStatusException {

    public HttpServerNotSupportException(int status, String message) {
        super(status, message);
    }

    public HttpServerNotSupportException(ResultResponse result) {
        super(result);
    }

    private static final long serialVersionUID = 3183070191347274019L;
    
}
