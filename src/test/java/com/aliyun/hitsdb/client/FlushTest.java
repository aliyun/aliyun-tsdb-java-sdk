package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Copyright @ 2020 alibaba.com
 * All right reserved.
 * Function：Flush Test
 *
 * @author Benedict Jin
 * @since 2020/8/27
 */
public class FlushTest {

    /**
     * {"timestamp":1}
     * {"timestamp":2}
     * {"timestamp":3}
     * {"timestamp":4}
     * {"timestamp":5}
     * [{"timestamp":1},{"timestamp":2}]
     * [{"timestamp":3},{"timestamp":4}]
     * [{"timestamp":5}]
     */
    @Test
    public void testFlush() throws Exception {
        int batchPutBufferSize = 1024;
        int batchPutSize = 2;
        BlockingQueue<Object> pointQueue = new ArrayBlockingQueue<Object>(batchPutBufferSize);
        final Point point1 = new Point();
        point1.setTimestamp(1L);
        pointQueue.put(point1);
        final Point point2 = new Point();
        point2.setTimestamp(2L);
        pointQueue.put(point2);
        final Point point3 = new Point();
        point3.setTimestamp(3L);
        pointQueue.put(point3);
        final Point point4 = new Point();
        point4.setTimestamp(4L);
        pointQueue.put(point4);
        final Point point5 = new Point();
        point5.setTimestamp(5L);
        pointQueue.put(point5);
        final Point[] points = pointQueue.toArray(new Point[0]);
        for (Point point : points) {
            System.out.println(point);
        }
        final ArrayList<Point> pointList = new ArrayList<Point>(points.length);
        Collections.addAll(pointList, points);
        for (int i = 0; i <= points.length - 1; i += batchPutSize) {
            final int endBound = Math.min(points.length, i + batchPutSize);
            final List<Point> sub = pointList.subList(i, endBound);
            System.out.println(JSON.toJSONString(sub));
        }
    }
}
