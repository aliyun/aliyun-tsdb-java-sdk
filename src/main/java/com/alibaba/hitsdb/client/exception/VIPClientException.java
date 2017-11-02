package com.alibaba.hitsdb.client.exception;

public class VIPClientException extends RuntimeException {

    private static final long serialVersionUID = -1311355061015731848L;
    
    public VIPClientException(){
    }
    
    public VIPClientException(Exception e){
        super(e);
    }
}
