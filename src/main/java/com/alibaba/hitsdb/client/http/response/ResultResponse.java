package com.alibaba.hitsdb.client.http.response;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.util.EntityUtils;

import com.alibaba.hitsdb.client.exception.http.HttpClientException;
import com.alibaba.hitsdb.client.http.HttpClient;

public class ResultResponse {
    private int statusCode;
    private HttpStatus httpStatus;
    private String content;
    private HttpResponse httpResponse;
    private boolean compress;

    public ResultResponse(int statusCode) {
        super();
        this.statusCode = statusCode;
        if (statusCode >= 200 && statusCode < 300) {
            if(statusCode == 204){
                this.httpStatus = HttpStatus.ServerSuccessNoContent;
            } else {
                this.httpStatus = HttpStatus.ServerSuccess;
            }
        } else if (statusCode >= 400 && statusCode < 500) {
            this.httpStatus = HttpStatus.ServerNotSupport;
        } else if (statusCode >= 500 && statusCode < 600) {
            this.httpStatus = HttpStatus.ServerError;
        } else {
            this.httpStatus = HttpStatus.UnKnow;
        }
    }

    public boolean isSuccess() {
        if (statusCode >= 200 && statusCode < 300) {
            return true;
        }
        return false;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public ResultResponse(int status, String content) {
        super();
        this.statusCode = status;
        this.content = content;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getContent() {
        if (this.content == null) {
            HttpEntity entity = this.httpResponse.getEntity();
            try {
                String content = null;
                Header[] headers = this.httpResponse.getHeaders("Content-Encoding");
                if (headers != null && headers.length > 0 && headers[0].getValue().equalsIgnoreCase("gzip")) {
                    GzipDecompressingEntity gzipEntity = new GzipDecompressingEntity(entity);
                    content = EntityUtils.toString(gzipEntity, HttpClient.DEFAULT_CHARSET);
                } else {
                    content = EntityUtils.toString(entity, HttpClient.DEFAULT_CHARSET);
                }

                this.content = content;
            } catch (ParseException e) {
                throw new HttpClientException(e);
            } catch (IOException e) {
                throw new HttpClientException(e);
            }
        }

        return content;
    }

    public boolean isCompress() {
        return compress;
    }

    public static ResultResponse simplify(HttpResponse httpResponse, boolean compress) {
        StatusLine statusLine = httpResponse.getStatusLine();
        int statusCode = statusLine.getStatusCode();
        ResultResponse resultResponse = new ResultResponse(statusCode);
        resultResponse.httpResponse = httpResponse;
        resultResponse.compress = compress;
        return resultResponse;
    }

}