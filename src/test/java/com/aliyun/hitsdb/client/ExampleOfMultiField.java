package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryLastResult;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class ExampleOfMultiField {
    TSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        TSDBConfig config = TSDBConfig.address("127.0.0.1", 8242)
                .httpConnectTimeout(90)
                .config();
        tsdb = TSDBClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            System.out.println("将要关闭");
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void exampleOfMultiFieldPutAndQuery() {
        /*
         *  通过 multiFieldPutSync() API。
         *  该 API 支持单点和多点(List)写入。
         *
         *  下面是必须写入时必须提供的信息:
         *  Metric: 数据指标的类别。相当于 influxdb 的 Measurement。
         *  Fields: 数据指标的度量信息（相当于指标下的子类别）。即一个 metric 支持多个 fields。如 metric 为 wind，该 metric 可以有多个 fields：direction, speed, direction, description 和 temperature。
         *  Timestamp: 数据点的时间戳
         *  Tags: 时间线额外的信息，如"型号=ABC123"、"出厂编号=1234567890"等。
         */
        MultiFieldPoint multiFieldPoint = MultiFieldPoint.metric("wind")
                .field("speed", 45.2)
                .field("level", 1.2)
                .field("direction", "S")
                .field("description", "Breeze")
                .tag("sensor", "95D8-7913")
                .tag("city", "hangzhou")
                .tag("province", "zhejiang")
                .timestamp(1537170208L)
                .build();
        // 同步写入
        tsdb.multiFieldPutSync(multiFieldPoint);

        /*
         *  通过multiFieldQuery() API。
         *  请求创建流程：MultiFieldSubQueryDetails List -> MultiFieldSubQuery -> MultiFieldQuery
         *  现在多值查询支持多个子查询。
         *
         *  查询时必须提供的信息:
         *  MultiFieldSubQueryDetails List: 查询指标下具体的度量（子类比）信息。例如只查询"speed"或者“direction”
         *                                  里面可以指定聚合运算（Aggreagtor），值过滤（dpValue），斜率计算（rate），降采样（downsample）等。
         *  Metric: 代表查询的数据指标，例如 "wind"
         *  Time Range: Start Time and End Time
         *
         *  可选信息：
         *  Tags: 过滤时间的信息
         *  Limit/Offset: 分页处理
         */
        // 创建要查询 fields 的信息
        // Query : Filter: level >= 1.2 & speed >= 45.2
        MultiFieldSubQueryDetails fieldDetail_1 = MultiFieldSubQueryDetails
                .field("speed").aggregator(Aggregator.NONE).dpValue(">=45.2").build();
        MultiFieldSubQueryDetails fieldDetail_2 = MultiFieldSubQueryDetails
                .field("level").aggregator(Aggregator.NONE).dpValue(">=1.2").build();
        MultiFieldSubQueryDetails fieldDetail_3 = MultiFieldSubQueryDetails
                .field("direction").aggregator(Aggregator.NONE).build();
        MultiFieldSubQueryDetails fieldDetail_4 = MultiFieldSubQueryDetails
                .field("description").aggregator(Aggregator.NONE).build();

        List<MultiFieldSubQueryDetails> fieldsDetails = new ArrayList();
        fieldsDetails.add(fieldDetail_1);
        fieldsDetails.add(fieldDetail_2);
        fieldsDetails.add(fieldDetail_3);
        fieldsDetails.add(fieldDetail_4);

        // 创建多值模型子查询
        MultiFieldSubQuery subQuery = MultiFieldSubQuery.metric("wind")
                .tag("sensor", "95D8-7913")
                .tag("city", "hangzhou")
                .tag("province", "zhejiang")
                .fieldsInfo(fieldsDetails)
                .build();

        // 创建多值模型查询
        MultiFieldQuery query = MultiFieldQuery.start(1537170208L).end(1537170209L)
                .sub(subQuery).build();

        List<MultiFieldQueryResult> result = tsdb.multiFieldQuery(query);
        if (result != null) {
            System.out.println("##### Multi-field Query Result : " + JSON.toJSONString(result));
            if (result.size() > 0) {
                System.out.println("##### Multi-field Query Result asMap : " + JSON.toJSONString(result.get(0).asMap()));
            }
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }

    }


    /**
     * @TCDescription : 多值模型写入
     * @TestStep :
     * @ExpectResult : 成功读取且数据一致
     * @author moqu
     * @modify by chixiao
     * @since 1.0.0
     */
    @Test
    public void exampleOfMultiFieldQueryLast() {
        /*
         *  multiFieldQueryLast() API。
         *  和单值模型查询方式类似，只是在创建 LastPointSubQuery 时候需要提供 fields 信息。
         *  查询时必须提供的信息:
         *  Metric: 代表查询的数据指标，例如"metric":"wind"。
         *  Fields: 查询指标下具体的度量（子类比）信息，例如 "speed", "level", "temperature"。
         */
        String metric = "wind";
        List<String> fields = new ArrayList<String>();
        fields.add("direction");
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("sensor", "95D8-7913");
        LastPointQuery lastPointQuery = LastPointQuery.builder()
                .sub(LastPointSubQuery.builder(metric, fields, tags).build()).tupleFormat(true).build();
        List<MultiFieldQueryLastResult> result = tsdb.multiFieldQueryLast(lastPointQuery);
        if (result != null) {
            System.out.println("##### Multi-field Query Last Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
    }

}
