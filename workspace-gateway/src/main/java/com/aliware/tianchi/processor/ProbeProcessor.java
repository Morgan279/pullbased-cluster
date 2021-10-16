package com.aliware.tianchi.processor;

import com.aliware.tianchi.entity.ProbeRecord;

public class ProbeProcessor {

    private static final int UPPER_BOUND = 100;

    private static final int LOWER_BOUND = 1;

    private int l = LOWER_BOUND;

    private int r = UPPER_BOUND;

    public volatile int bound = l + (r - l + 1) / 3;

    private LastStatus lastStatus = LastStatus.RIGHT;

    private final ProbeRecord left = new ProbeRecord(bound, 0);

    private final ProbeRecord right = new ProbeRecord(bound, 0);
//
//    private final ProbeRecord lastDown = new ProbeRecord(bound, lastComputingRate);

    public boolean isDraining() {
        return LastStatus.DRAIN.equals(lastStatus);
    }

    public boolean onConverge(double newestComputingRate) {
        if (LastStatus.RIGHT.equals(lastStatus)) {
            //现在拿到新的左值
            left.rate = newestComputingRate;
            //探测右值
            bound = right.bound = r - (r - l + 1) / 3;
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
            lastStatus = LastStatus.RIGHT;
        }
//        else {
//            bound >>= 1;
//            lastStatus = LastStatus.DRAIN;
//        }

        return r - l < 3;
    }

    public void probe() {
//        l = Math.max(l - 30, LOWER_BOUND);
//        r = Math.min(r + 30, UPPER_BOUND);
        l = LOWER_BOUND;
        r = UPPER_BOUND;
        bound = left.bound = l + (r - l + 1) / 3;
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
