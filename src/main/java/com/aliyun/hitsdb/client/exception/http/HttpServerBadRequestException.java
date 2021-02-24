package com.aliyun.hitsdb.client.exception.http;

import com.aliyun.hitsdb.client.http.response.ResultResponse;

// originally, the 400 was returned with HttpServerNotSupportException
// as a result, let HttpServerBadRequestException extend from HttpServerNotSupportException for the backward compatibility
public class HttpServerBadRequestException extends HttpServerNotSupportException {
    public HttpServerBadRequestException(ResultResponse result) {
        super(result);
    }
}
