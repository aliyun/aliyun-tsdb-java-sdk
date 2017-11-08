package com.aliyun.hitsdb.client.http.request;

import java.net.URI;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

public class HttpDeleteWithEntity extends HttpEntityEnclosingRequestBase {
    public final static String METHOD_NAME = "DELETE";
    
    public HttpDeleteWithEntity() {
        super();
    }

    public HttpDeleteWithEntity(final URI uri) {
        super();
        setURI(uri);
    }

    /**
     * @throws IllegalArgumentException if the uri is invalid.
     */
    public HttpDeleteWithEntity(final String uri) {
        super();
        setURI(URI.create(uri));
    }

    @Override
    public String getMethod() {
        return METHOD_NAME;
    }
}
