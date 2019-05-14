package com.aliyun.hitsdb.client.exception;

public class BufferQueueFullException extends RuntimeException {
    private static final long serialVersionUID = -6089496689881016447L;

    public BufferQueueFullException(String message, Throwable cause) {
        super(message, cause);
    }

}
