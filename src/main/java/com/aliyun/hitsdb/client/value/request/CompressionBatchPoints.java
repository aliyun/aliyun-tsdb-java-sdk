package com.aliyun.hitsdb.client.value.request;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSONObject;

import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayOutput;
import fi.iki.yak.ts.compression.gorilla.Pair;

public class CompressionBatchPoints {
    private CompressionBatchPoints () {}
    
    public static class MetricBuilder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();
        private List<Pair> pairs = new LinkedList<Pair>();

        public MetricBuilder() {}

        public MetricBuilder(final String metric) {
            this.metric = metric;
        }

        /**
         * add a TagKey and TagValue
         * 
         * @param tagName tagName
         * @param value value
         * @return MetricBuilder
         */
        public MetricBuilder tag(final String tagName, final String value) {
            Objects.requireNonNull(tagName, "tagName");
            Objects.requireNonNull(value, "value");
            if (!tagName.isEmpty() && !value.isEmpty()) {
                tags.put(tagName, value);
            }
            return this;
        }

        /**
         * add the tags
         * 
         * @param tags a map
         * @return MetricBuilder
         */
        public MetricBuilder tag(final Map<String, String> tags) {
            this.tags.putAll(tags);
            return this;
        }

        public MetricBuilder appendDouble(long timestamp, double value) {
            long doubleToRawLongBits = Double.doubleToRawLongBits(value);
            this.pairs.add(new Pair(timestamp, doubleToRawLongBits));
            return this;
        }

        public MetricBuilder appendLong(long timestamp, long value) {
            this.pairs.add(new Pair(timestamp, value));
            return this;
        }

        /**
         * build a point
         * 
         * @return Point
         */
        public CompressionBatchPoints build() {
            return build(true);
        }

        public CompressionBatchPoints build(boolean checkPoint) {
            CompressionBatchPoints points = new CompressionBatchPoints();
            points.metric = this.metric;
            points.tags = this.tags;
            points.pairs = this.pairs;
            return points;
        }
    }
    
    public static MetricBuilder metric(String metric) {
        MetricBuilder metricBuilder = new MetricBuilder();
        metricBuilder.metric = metric;
        return metricBuilder;
    }

    private String metric;
    private Map<String, String> tags = new HashMap<String, String>();
    private List<Pair> pairs = new LinkedList<Pair>();
    private byte[] compressData;

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public int size() {
        return pairs.size();
    }

    public Pair get(int index) {
        return pairs.get(index);
    }

    public void compressTS() throws IOException {
        JSONObject json = new JSONObject();
        json.put("metric", this.metric);
        json.put("tags", this.tags);
        if(this.pairs != null || this.pairs.size() > 0) {
            Pair pair = this.pairs.get(0);
            json.put("timestamp", pair.getTimestamp());
        }
        String jsonString = json.toJSONString();
        
        LongArrayOutput output = new LongArrayOutput();
        if (this.size() > 0) {
            Pair pair0 = this.get(0);
            GorillaCompressor c = new GorillaCompressor(pair0.getTimestamp(), output);

            for (int i = 0; i < this.size(); i++) {
                Pair pair = this.get(i);
                c.addValue(pair.getTimestamp(), pair.getLongValue());
            }

            c.close();
        }

        long[] longArray = output.getLongArray();
        ByteArrayOutputStream ba = new ByteArrayOutputStream(4 + jsonString.length() + longArray.length * 8);
        DataOutputStream da = new DataOutputStream(ba);
        
        // 写长度
        da.writeInt(jsonString.length());
        
        // 写json部分
        byte[] bytes = jsonString.getBytes();
        da.write(bytes);
        
        // 写数据
        appendBytes(da, longArray);
        byte[] rtn = ba.toByteArray();
        this.compressData = rtn;
    }

    private void appendBytes(DataOutputStream da, long[] input) throws IOException {
        for (long v : input) {
            da.writeLong(v);
        }
    }

    public byte[] getCompressData() {
        return compressData;
    }

}
