package com.alibaba.hitsdb.client.exception.http;

//下一个大版本，移动到com.alibaba.hitsdb.client.exception.http.client
public class HttpClientException extends RuntimeException {

    public HttpClientException() {
    }

    public HttpClientException(Exception e) {
        super(e);
    }
    
    public HttpClientException(String messsage) {
        super(messsage);
    }
    
    public HttpClientException(String messsage,Exception e) {
        super(messsage,e);
    }

    private static final long serialVersionUID = -2345036415069363458L;
}