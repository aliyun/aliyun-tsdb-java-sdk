package com.aliyun.hitsdb.client.value.request;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.hitsdb.client.value.JSONValue;

public class BatchPoints extends JSONValue {
    public static class Builder {
        private List<Point> points = new LinkedList<Point>();

        public Builder(Point... points) {
            for (Point point : points) {
                this.points.add(point);
            }
        }

        public Builder add(Point... point) {
            return new Builder(point);
        }

        public BatchPoints build() {
            BatchPoints batchPoints = new BatchPoints();
            batchPoints.points = this.points;
            return batchPoints;
        }
    }

    private List<Point> points = new LinkedList<Point>();

    public static Builder add(Point... point) {
        return new Builder(point);
    }

    public Iterator<Point> iterator() {
        Iterator<Point> iterator = this.points.iterator();
        return iterator;
    }
}
