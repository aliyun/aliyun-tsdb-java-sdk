package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.annotation.JSONType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

@Deprecated
@JSONType(ignores = { "timestamp" })
public class MultiValuedTupleComparator implements Comparator<List<Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiValuedTupleComparator.class);

    public Boolean reverseOrder = false;

    public MultiValuedTupleComparator() {

    }

    public MultiValuedTupleComparator(Boolean reverseOrder) {
        this.reverseOrder = reverseOrder;
    }

    @Override
    public int compare(List<Object> tupleA, List<Object> tupleB) {
        /**
         * Comparison procedure:
         * 1. Timestamp --- The first field value of the tuple
         * 2. Tuple field values (toString())
         */
        if (tupleA == null || tupleA.isEmpty() || tupleB == null || tupleB.isEmpty()) {
            LOGGER.error("Unable to perform these two tuples. {} ||| {}", tupleA == null ? "NULL" : tupleA.toString(),
                    tupleB == null ? "NULL" : tupleB.toString());
            return 0;
        }
        int comparingResult = 0;

        Long timestampA = (Long) tupleA.get(0);
        Long timestampB = (Long) tupleB.get(0);

        if (!timestampA.equals(timestampB)) {
            comparingResult = (int)(timestampA - timestampB);
        } else {
            if (tupleA.size() != tupleB.size()) {
                comparingResult = (tupleA.size() - tupleB.size());
            } else {
                comparingResult = (tupleA.toString().compareTo(tupleB.toString()));
            }
        }

        if (reverseOrder) {
            comparingResult = (negateExact(comparingResult));
        }
        return comparingResult;
    }


    public static int negateExact(int a) {
        if (a == Integer.MIN_VALUE) {
            throw new ArithmeticException("integer overflow");
        }

        return -a;
    }
}
