package com.aliyun.hitsdb.client.exception.http;

import com.aliyun.hitsdb.client.http.response.ResultResponse;

public class HttpServerUnauthorizedException extends HttpUnknowStatusException {
    private static final long serialVersionUID = -8842332621020945130L;

    public HttpServerUnauthorizedException(int status, String message) { super(status, message); }

    public HttpServerUnauthorizedException(ResultResponse result) { super(result); }
}
