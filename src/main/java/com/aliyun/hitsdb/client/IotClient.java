package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.response.HttpStatus;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.value.request.TagsAddInfo;
import com.aliyun.hitsdb.client.value.request.TagsRemoveInfo;
import com.aliyun.hitsdb.client.value.request.TagsShowInfo;
import com.aliyun.hitsdb.client.value.response.TagsAddResult;
import com.aliyun.hitsdb.client.value.response.TagsShowResult;
import org.apache.http.HttpResponse;

import java.util.List;

/**
 * @author johnnyzou
 */
public class IotClient extends TSDBClient{

    public IotClient(Config config) throws HttpClientInitException {
        super(config);
    }

    /**
     * Add tags for id in Super Tag mode.
     *  @param tagsAddInfo the attached tags for given _id list
     *  @return http status 200 does not mean all _id has successfully attached for given tag
     */
    public TagsAddResult tagsAdd(TagsAddInfo tagsAddInfo) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.TAGS_ADD, tagsAddInfo.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccess:
                String content = resultResponse.getContent();
                TagsAddResult result = JSON.parseObject(content, TagsAddResult.class);
                return result;
            default:
                return (TagsAddResult) handleStatus(resultResponse);
        }

    }

    /**
     * Show tags by id in Super Tag mode.
     * @param tagsShowInfo the metric and _id which show tags needed
     * @return the tags info list of given _ids
     */
    public List<TagsShowResult> tagsShow(TagsShowInfo tagsShowInfo) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.TAGS_SHOW, tagsShowInfo.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<TagsShowResult> result = JSON.parseArray(content, TagsShowResult.class);
                return result;
            default:
                return ( List<TagsShowResult>) handleStatus(resultResponse);
        }
    }

    /**
     * Remove tags by id or id + tags in Super Tag mode.
     * @param tagsRemoveInfo the metric, _id and tags(can be null) which remove tags needed
     * @return
     */
    public void tagsRemove(TagsRemoveInfo tagsRemoveInfo) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.TAGS_REMOVE, tagsRemoveInfo.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }
}
