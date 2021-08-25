package com.aliyun.hitsdb.client.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.aliyun.hitsdb.client.value.request.AbstractPoint;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.PointsCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.hitsdb.client.exception.BufferQueueFullException;
import com.aliyun.hitsdb.client.value.request.Point;

public class DataPointQueue implements DataQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointQueue.class);

    /**
     * each queue has a corresponding thread pool to consume the points
     *   pointQueue            - BatchPutRunnable
     *   multiFieldPointQueue  - MultiFieldBatchPutRunnable
     *   pointsCollectionQueue - PointsCollectionPutRunnable
     */
    private final BlockingQueue<Point> pointQueue;
    private final BlockingQueue<MultiFieldPoint> multiFieldPointQueue;

    // pointsCollectionQueue is designed for the async put interfaces,
    // which triggers a specified callback every time when a collection sent.
    private final BlockingQueue<PointsCollection> pointsCollectionQueue;

    private final AtomicBoolean forbiddenWrite = new AtomicBoolean(false);
    private final int waitCloseTimeLimit;
    private final boolean backpressure;

    public DataPointQueue(int batchPutBufferSize, int multiFieldBatchPutBufferSize, int waitCloseTimeLimit, boolean backpressure) {
        if (batchPutBufferSize <= 0) {
            batchPutBufferSize = 1;
        }
        if (multiFieldBatchPutBufferSize <= 0) {
            multiFieldBatchPutBufferSize = 1;
        }
        this.pointQueue = new ArrayBlockingQueue<Point>(batchPutBufferSize);
        this.multiFieldPointQueue = new ArrayBlockingQueue<MultiFieldPoint>(multiFieldBatchPutBufferSize);

        this.pointsCollectionQueue = new ArrayBlockingQueue(Math.max(batchPutBufferSize, multiFieldBatchPutBufferSize));

        this.waitCloseTimeLimit = waitCloseTimeLimit;
        this.backpressure = backpressure;
    }

    @Override
    public void send(Point point) {
        verifyWrite();
        if (this.backpressure) {
            try {
                pointQueue.put(point);
            } catch (InterruptedException e) {
                LOGGER.error("Client Thread been Interrupted.", e);
            }
        } else {
            try {
                pointQueue.add(point);
            } catch (IllegalStateException exception) {
                throw new BufferQueueFullException("The buffer queue is full.", exception);
            }
        }
    }

    private void verifyWrite() {
        if (forbiddenWrite.get()) {
            throw new IllegalStateException("client has been closed.");
        }
    }

    @Override
    public Point receive() throws InterruptedException {
        return pointQueue.take();
    }

    @Override
    public Point receive(int timeout) throws InterruptedException {
       return pointQueue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendMultiFieldPoint(MultiFieldPoint point) {
        verifyWrite();
        if (this.backpressure) {
            try {
                multiFieldPointQueue.put(point);
            } catch (InterruptedException e) {
                LOGGER.error("Client Thread been Interrupted.", e);
            }
        } else {
            try {
                multiFieldPointQueue.add(point);
            } catch (IllegalStateException exception) {
                throw new BufferQueueFullException("The buffer queue is full.", exception);
            }
        }
    }

    @Override
    public MultiFieldPoint receiveMultiFieldPoint() throws InterruptedException {
        return multiFieldPointQueue.take();
    }

    @Override
    public MultiFieldPoint receiveMultiFieldPoint(int timeout) throws InterruptedException {
        return multiFieldPointQueue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sendPoints(PointsCollection points) {
        verifyWrite();
        if (this.backpressure) {
            try {
                pointsCollectionQueue.put(points);
            } catch (InterruptedException e) {
                LOGGER.error("Client Thread been Interrupted.", e);
            }
        } else {
            try {
                pointsCollectionQueue.add(points);
            } catch (IllegalStateException exception) {
                throw new BufferQueueFullException("The buffer queue is full.", exception);
            }
        }
    }

    @Override
    public PointsCollection receivePoints() throws InterruptedException {
        return pointsCollectionQueue.take();
    }

    @Override
    public PointsCollection receivePoints(int timeout) throws InterruptedException {
        return pointsCollectionQueue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void forbiddenSend() {
        forbiddenWrite.compareAndSet(false, true);
    }

    @Override
    public void waitEmpty() {
        // 等待为空之前，必须已经设置了禁止写入
        if (forbiddenWrite.get()) {
            try {
                Thread.sleep(waitCloseTimeLimit);
            } catch (InterruptedException e) {
                LOGGER.warn("The method waitEmpty() is being illegally interrupted");
            }

            while (true) {
                boolean empty = pointQueue.isEmpty();
                boolean multiEmpty = multiFieldPointQueue.isEmpty();
                boolean asyncQueueEmpty = pointsCollectionQueue.isEmpty();
                if (empty && multiEmpty && asyncQueueEmpty) {
                    return;
                } else {
                    try {
                        Thread.sleep(waitCloseTimeLimit);
                    } catch (InterruptedException e) {
                        LOGGER.warn("The waitEmpty() method is being illegally interrupted");
                    }
                }
            }
        } else {
            throw new IllegalStateException(
                    "The queue is still allowed to write data. you must first call the forbiddenSend() method");
        }
    }

    @Override
    public boolean isEmpty() {
        return pointQueue.isEmpty() && multiFieldPointQueue.isEmpty() && pointsCollectionQueue.isEmpty();
    }

    @Override
    public Point[] getPoints() {
        return pointQueue.toArray(new Point[0]);
    }

    @Override
    public MultiFieldPoint[] getMultiFieldPoints() {
        return multiFieldPointQueue.toArray(new MultiFieldPoint[0]);
    }
}