package com.aliyun.hitsdb.client.value.request;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSONObject;

import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayOutput;

public class CompressionBatchPoints {
    private CompressionBatchPoints() {}

    public static class MetricBuilder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();
        private LongArrayOutput output = new LongArrayOutput();
        private GorillaCompressor compressor;
        private int size = 0;
        private long timestamp;

        public MetricBuilder() {}

        public MetricBuilder(final String metric) {
            this.metric = metric;
        }

        public String getMetric() {
            return this.metric;
        }

        public Map<String, String> getTags() {
            return this.tags;
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
            if (this.compressor == null) {
                this.timestamp = timestamp;
                this.compressor = new GorillaCompressor(timestamp, output);
            }
            size++;
            this.compressor.addValue(timestamp, value);
            return this;
        }

        public MetricBuilder appendLong(long timestamp, long value) {
            if (this.compressor == null) {
                this.timestamp = timestamp;
                this.compressor = new GorillaCompressor(timestamp, output);
            }
            size++;
            this.compressor.addValue(timestamp, value);
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
            points.output = this.output;
            if (this.compressor != null) {
                this.compressor.close();
            }
            points.timestamp = timestamp;
            points.size = this.size;
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
    private byte[] compressData;
    private LongArrayOutput output;
    private int size;
    private long timestamp;

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public int size() {
        return this.size;
    }

    public void compressTS() throws IOException {
        if (size <= 0) {
            throw new IOException("compress size <= 0");
        }
        JSONObject json = new JSONObject();
        json.put("metric", this.metric);
        json.put("tags", this.tags);
        json.put("timestamp", timestamp);
        json.put("size", this.size);

        String jsonString = json.toJSONString();
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

    public int getSize() {
        return size;
    }

}