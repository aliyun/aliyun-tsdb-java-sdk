package com.alibaba.hitsdb.client.exception.http;

/**
 * @author shijiaqi
 */
// 下一个大版本，移动到com.alibaba.hitsdb.client.exception.http.client
public class HttpClientConnectionRefusedException extends HttpClientException {

    private static final long serialVersionUID = -4155566481009479303L;
    
    public HttpClientConnectionRefusedException(String address,Exception e) {
        super(e);
    }

}
