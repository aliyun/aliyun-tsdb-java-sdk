package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.util.Objects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PointsCollection extends ArrayList<AbstractPoint> {
    @JSONField(serialize = false)
    private final AbstractBatchPutCallback  simplePointBatchCallbak;
    @JSONField(serialize = false)
    private final AbstractMultiFieldBatchPutCallback  multiFieldBatchPutCallback;

    public PointsCollection(Collection<Point> points, AbstractBatchPutCallback callback) {
        Objects.requireNonNull(points);
        Objects.requireNonNull(callback);

        addAll(points);
        this.simplePointBatchCallbak = callback;
        this.multiFieldBatchPutCallback = null;
    }

    public PointsCollection(Collection<MultiFieldPoint> multiFieldPoints, AbstractMultiFieldBatchPutCallback callback) {
        Objects.requireNonNull(multiFieldPoints);
        Objects.requireNonNull(callback);

        addAll(multiFieldPoints);
        this.simplePointBatchCallbak = null;
        this.multiFieldBatchPutCallback = callback;
    }

    public AbstractBatchPutCallback getSimplePointBatchCallbak() {
        return simplePointBatchCallbak;
    }

    public AbstractMultiFieldBatchPutCallback getMultiFieldBatchPutCallback() {
        return multiFieldBatchPutCallback;
    }

    public List<Point> asSingleFieldPoints() {
        List<Point> retval = new ArrayList<Point>(this.size());
        for (AbstractPoint p : this) {
            if (!(p instanceof Point)) {
                throw new IllegalStateException("it's not a SingleFieldPoint collection");
            }
            retval.add((Point)p);
        }
        return retval;
    }

    public List<MultiFieldPoint> asMultiFieldPoints() {
        List<MultiFieldPoint> retval = new ArrayList<MultiFieldPoint>(this.size());
        for (AbstractPoint p : this) {
            if (!(p instanceof MultiFieldPoint)) {
                throw new IllegalStateException("it's not a MultiFieldPoint collection");
            }
            retval.add((MultiFieldPoint)p);
        }
        return retval;
    }

    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.SortField, SerializerFeature.SortField.MapSortField);
    }
}
