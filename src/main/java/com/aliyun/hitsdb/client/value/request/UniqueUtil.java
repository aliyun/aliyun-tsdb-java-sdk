package com.aliyun.hitsdb.client.value.request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author cuiyuan
 * @date 2021/2/10 3:13 下午
 */
public class UniqueUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueUtil.class);

    private static long hash(Object... values) {
        return Arrays.hashCode(values);
    }

    static long hash(MultiFieldPoint multiFieldPoint, String field) {
        return hash(multiFieldPoint.getMetric(), multiFieldPoint.getTags(), multiFieldPoint.getTimestamp(), field);
    }

    static long hash(Point point) {
        return hash(point.getMetric(), point.getTags(), point.getTimestamp());
    }

    public static void uniqueMultiFieldPoints(Collection<MultiFieldPoint> multiFieldPointList, boolean deduplicationEnable) {
        if (!deduplicationEnable) {
            return;
        }
        try {
            HashMap<Long, List<MultiFieldPoint>> checkSumToPoints = new HashMap<Long, List<MultiFieldPoint>>(multiFieldPointList.size());
            Iterator<MultiFieldPoint> multiFieldPointIterator = multiFieldPointList.iterator();
            while (multiFieldPointIterator.hasNext()) {
                MultiFieldPoint multiFieldPoint = multiFieldPointIterator.next();
                Iterator<Map.Entry<String, Object>> fieldIterator = multiFieldPoint.getFields().entrySet().iterator();
                while (fieldIterator.hasNext()) {
                    Map.Entry<String, Object> field = fieldIterator.next();
                    //算出每个field，对应的checksum
                    long checksum = hash(multiFieldPoint, field.getKey());
                    //如果hash值相同，为了避免hash碰撞但是并不冲突的情况(小概率事件)，还需要再做一次检查, 这里采用类似于链地址法来解决冲突
                    if (checkSumToPoints.containsKey(checksum)) {
                        List<MultiFieldPoint> oldPoints = checkSumToPoints.get(checksum);
                        if (multiFieldPointsHasSame(oldPoints, multiFieldPoint, field.getKey())) {
                            //确实是冲突的，移除这个field
                            fieldIterator.remove();
                        } else {
                            //并没有冲突，添加到points列表中
                            LOGGER.debug("checksum {} same but not conflict {} {} field {}", checksum, oldPoints, multiFieldPoint, field.getKey());
                            oldPoints.add(multiFieldPoint);
                        }
                    } else {
                        //并没有hash值相同的，说明没有冲突
                        List<MultiFieldPoint> pointList = new ArrayList<MultiFieldPoint>();
                        pointList.add(multiFieldPoint);
                        checkSumToPoints.put(checksum, pointList);
                    }
                }
                if (multiFieldPoint.getFields().size() == 0) {
                    //如果这个point的所有field都有重复被移除了，就移除这个point
                    multiFieldPointIterator.remove();
                }
            }
        } catch (Exception e) {
            LOGGER.error("ERROR occurred when uniqueMultiFieldPoints {} {}", multiFieldPointList, e);
        }
    }

    private static boolean pointsHasSame(Collection<Point> oldPoints, Point point) {
        for (Point oldPoint : oldPoints) {
            if (pointSame(oldPoint, point)) {
                return true;
            }
        }
        return false;
    }

    static boolean pointSame(Point oldPoint, Point point) {
        //metric
        if (!oldPoint.getMetric().equals(point.getMetric())) {
            return false;
        }
        //timestamp
        if (oldPoint.getTimestamp().longValue() != point.getTimestamp().longValue()) {
            return false;
        }
        //tags
        if (!tagsSame(oldPoint.getTags(), point.getTags())) {
            return false;
        }
        //全部相同则表示相同
        LOGGER.info("Point {} and {} conflict", oldPoint, point);
        return true;
    }

    static boolean tagsSame(Map<String, String> oldTags, Map<String, String> tags) {
        if (oldTags.size() != tags.size()) {
            return false;
        }

        for (Map.Entry<String, String> tagKv : tags.entrySet()) {
            String tagK = tagKv.getKey();
            String tagV = tagKv.getValue();
            if (!oldTags.containsKey(tagK)) {
                return false;
            }
            if (!oldTags.get(tagK).equals(tagV)) {
                return false;
            }
        }

        return true;
    }

    private static boolean multiFieldPointsHasSame(Collection<MultiFieldPoint> oldPoints, MultiFieldPoint point, String field) {
        for (MultiFieldPoint oldPoint : oldPoints) {
            if (multiFieldPointSame(oldPoint, point, field)) {
                return true;
            }
        }
        return false;
    }

    static boolean multiFieldPointSame(MultiFieldPoint oldPoint, MultiFieldPoint point, String field) {
        //field
        if (!oldPoint.getFields().containsKey(field)) {
            return false;
        }
        //metric
        if (!oldPoint.getMetric().equals(point.getMetric())) {
            return false;
        }
        //timestamp
        if (oldPoint.getTimestamp().longValue() != point.getTimestamp().longValue()) {
            return false;
        }
        //tags
        if( !tagsSame(oldPoint.getTags(), point.getTags())) {
            return false;
        }
        //全部相同则表示相同
        LOGGER.info("MultiFieldPoint {} and {} conflict", oldPoint, point);
        return true;
    }

    public static void uniquePoints(Collection<Point> points, boolean deduplicationEnable) {
        if (!deduplicationEnable) {
            return;
        }
        try {
            HashMap<Long, List<Point>> checkSumToPoints = new HashMap<Long, List<Point>>(points.size());
            Iterator<Point> pointIterator = points.iterator();
            while (pointIterator.hasNext()) {
                final Point point = pointIterator.next();
                long checksum = hash(point);
                if (checkSumToPoints.containsKey(checksum)) {
                    List<Point> oldPoints = checkSumToPoints.get(checksum);
                    if (pointsHasSame(oldPoints, point)) {
                        pointIterator.remove();
                    } else {
                        LOGGER.debug("checksum {} same but not conflict {} {}", checksum,  oldPoints, point);
                        oldPoints.add(point);
                    }
                } else {
                    List<Point> pointList = new ArrayList<Point>();
                    pointList.add(point);
                    checkSumToPoints.put(checksum, pointList);
                }
            }
        } catch (Exception e) {
            LOGGER.error("ERROR occurred when uniquePoints {} {}", points, e);
        }
    }
}
