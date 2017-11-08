package com.aliyun.hitsdb.client.exception.http;

public class HttpClientSocketTimeoutException extends HttpClientException {
    private static final long serialVersionUID = 5407907664312068303L;
    
    public HttpClientSocketTimeoutException(Exception e) {
        super(e);
    }
}
