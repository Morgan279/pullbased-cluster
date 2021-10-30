package com.aliware.tianchi.processor;

import com.aliware.tianchi.entity.ProbeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProbeProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(ProbeProcessor.class);

    private static final int UPPER_BOUND = 40;

    private static final int LOWER_BOUND = 120;

    private static final double[] LEFT_GAIN_VALUES = {0.75, 1, 1, 1, 1, 1, 1, 1};

    private static final double[] RIGHT_GAIN_VALUES = {1, 1, 1, 1, 1, 1, 1, 1};

    public volatile double[] gains = LEFT_GAIN_VALUES;

    private int l = LOWER_BOUND;

    private int r = UPPER_BOUND;

    public volatile int bound = l + (r - l + 1) / 3;

    private LastStatus lastStatus = LastStatus.RIGHT;

    private final ProbeRecord left = new ProbeRecord(bound, 0);

    private final ProbeRecord right = new ProbeRecord(bound, 0);
//
//    private final ProbeRecord lastDown = new ProbeRecord(bound, lastComputingRate);

    public boolean isProbingLeft() {
        return LastStatus.RIGHT.equals(lastStatus);
    }

    public boolean onConverge(double newestComputingRate) {
        if (LastStatus.RIGHT.equals(lastStatus)) {
            //现在拿到新的左值
            left.rate = newestComputingRate;
            //探测右值
            bound = right.bound = r - (r - l + 1) / 3;
            gains = RIGHT_GAIN_VALUES;
            lastStatus = LastStatus.LEFT;
        } else if (LastStatus.LEFT.equals(lastStatus)) {
            //现在拿到右值
            right.rate = newestComputingRate;
            if (left.rate < right.rate) {
                l = left.bound;
            } else {
                r = right.bound;
            }
            //探测新的左值
            bound = left.bound = l + (r - l + 1) / 3;
            gains = LEFT_GAIN_VALUES;
            lastStatus = LastStatus.RIGHT;
        }
//        else {
//            bound >>= 1;
//            lastStatus = LastStatus.DRAIN;
//        }
//        LOGGER.info("l:{} r:{}", l, r);
        return r - l < 3;
    }

    public void probe() {
        l = Math.max(l - 3, LOWER_BOUND);
        r = Math.min(r + 3, UPPER_BOUND);
//        l = LOWER_BOUND;
//        r = UPPER_BOUND;
        bound = left.bound = l + (r - l + 1) / 3;
        gains = LEFT_GAIN_VALUES;
        this.lastStatus = LastStatus.RIGHT;
    }

//    public void onConverge(double newestComputingRate) {
//        if (newestComputingRate > lastComputingRate) {
//            if (LastStatus.UP.equals(lastStatus)) {
//                double delta = newestComputingRate / lastComputingRate;
//                lowerBound = lastUpBound;
//                lastUpBound = bound;
//                bound *= Math.max(1 + delta, 1.1);
//            } else {
//                bound = (bound + lowerBound) >> 1;
//            }
//            lastStatus = LastStatus.UP;
//        } else {
//            upperBound = Math.min(upperBound, bound);
//            bound -= 3;
//            lastStatus = LastStatus.DOWN;
//        }
//
//        lastComputingRate = newestComputingRate;
//    }

    private enum LastStatus {
        UP,
        DOWN,
        LEFT,
        RIGHT,
        DRAIN
    }

}
