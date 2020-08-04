package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

/**
 * Copyright @ 2020 alibaba.com
 * All right reserved.
 * Function：HiTSDBClient Test
 *
 * @author Benedict Jin
 * @since 2020/6/3
 */
public class TSDBClientTest {

    @Before
    public void setup() {
        JSON.DEFAULT_PARSER_FEATURE &= ~Feature.UseBigDecimal.getMask();
    }

    @Test
    public void testDataTypeWithSingleLongDataPoint() {
        final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1}}]";
        final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        Assert.assertEquals(1, queryResultList.size());

        final Query query = new Query();
        query.setShowType(true);
        TSDBClient.setTypeIfNeeded(query, queryResultList);
        Assert.assertEquals(Long.class, queryResultList.get(0).getType());
    }

    @Test
    public void testDataTypeWithSingleDoubleDataPoint() {
        {
            final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1.0}}]";
            final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
            Assert.assertEquals(1, queryResultList.size());

            final Query query = new Query();
            query.setShowType(true);
            TSDBClient.setTypeIfNeeded(query, queryResultList);
            Assert.assertEquals(Double.class, queryResultList.get(0).getType());
        }
        {
            final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1.1}}]";
            final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
            Assert.assertEquals(1, queryResultList.size());

            final Query query = new Query();
            query.setShowType(true);
            TSDBClient.setTypeIfNeeded(query, queryResultList);
            Assert.assertEquals(Double.class, queryResultList.get(0).getType());
        }
    }

    @Test
    public void testDataTypeWithMultiLongDataPoints() {
        final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1,\"1346846401\":2,\"1346846402\":3}}]";
        final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        Assert.assertEquals(1, queryResultList.size());

        final Query query = new Query();
        query.setShowType(true);
        TSDBClient.setTypeIfNeeded(query, queryResultList);
        Assert.assertEquals(Long.class, queryResultList.get(0).getType());
    }

    @Test
    public void testDataTypeWithMultiDoubleDataPoints() {
        {
            final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1.0,\"1346846401\":2.0,\"1346846402\":3.0}}]";
            final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
            Assert.assertEquals(1, queryResultList.size());

            final Query query = new Query();
            query.setShowType(true);
            TSDBClient.setTypeIfNeeded(query, queryResultList);
            Assert.assertEquals(Double.class, queryResultList.get(0).getType());
        }
        {
            final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1.1,\"1346846401\":2.2,\"1346846402\":3.3}}]";
            final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
            Assert.assertEquals(1, queryResultList.size());

            final Query query = new Query();
            query.setShowType(true);
            TSDBClient.setTypeIfNeeded(query, queryResultList);
            Assert.assertEquals(Double.class, queryResultList.get(0).getType());
        }
    }

    @Test
    public void testDataTypeWithMixedLongAndDoubleDataPoints() {
        final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1,\"1346846401\":2.0,\"1346846402\":3.3}}]";
        final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        Assert.assertEquals(1, queryResultList.size());

        final Query query = new Query();
        query.setShowType(true);
        TSDBClient.setTypeIfNeeded(query, queryResultList);
        Assert.assertEquals(BigDecimal.class, queryResultList.get(0).getType());
    }

    @Test
    public void testDataTypeWithBooleanDataPoint() {
        final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":true}}]";
        final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        Assert.assertEquals(1, queryResultList.size());

        final Query query = new Query();
        query.setShowType(true);
        TSDBClient.setTypeIfNeeded(query, queryResultList);
        Assert.assertEquals(Boolean.class, queryResultList.get(0).getType());
    }

    @Test
    public void testDataTypeWithStringDataPoint() {
        final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":\"1\"}}]";
        final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        Assert.assertEquals(1, queryResultList.size());

        final Query query = new Query();
        query.setShowType(true);
        TSDBClient.setTypeIfNeeded(query, queryResultList);
        Assert.assertEquals(String.class, queryResultList.get(0).getType());
    }

    @Test
    public void testDataTypeWithInvalidDataPoint() {
        final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":[1,2,3]}}]";
        final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        Assert.assertEquals(1, queryResultList.size());

        final Query query = new Query();
        query.setShowType(true);
        TSDBClient.setTypeIfNeeded(query, queryResultList);
        Assert.assertEquals(Object.class, queryResultList.get(0).getType());
    }

    @Test
    public void testDataTypeWithMixedTimelines() {
        final String content = "[{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"asdf2014\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1}},{\"metric\":\"yuzhouwan\",\"tags\":{\"name\":\"BenedictJin\"},\"aggregateTags\":[],\"dps\":{\"1346846400\":1,\"1346846401\":2.0}}]";
        final List<QueryResult> queryResultList = JSON.parseArray(content, QueryResult.class);
        Assert.assertEquals(2, queryResultList.size());

        final Query query = new Query();
        query.setShowType(true);
        TSDBClient.setTypeIfNeeded(query, queryResultList);
        Assert.assertEquals(Long.class, queryResultList.get(0).getType());
        Assert.assertEquals(BigDecimal.class, queryResultList.get(1).getType());
    }
}
