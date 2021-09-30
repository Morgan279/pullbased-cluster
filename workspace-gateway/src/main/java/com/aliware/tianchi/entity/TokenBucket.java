package com.aliware.tianchi.entity;

import java.util.concurrent.TimeUnit;

public class TokenBucket {

    private double storedPermits;

    private volatile double grantInterval;

    private long elapsedNanos;

    private long lastAcquireNanoSec;

    private long nextFreeTime;

    public volatile double pacingGain;

    public TokenBucket(double grantInterval, double pacingGain) {
        this.grantInterval = grantInterval;
        this.pacingGain = pacingGain;
        this.storedPermits = 0D;
        this.elapsedNanos = 0L;
        this.nextFreeTime = 0L;
        this.lastAcquireNanoSec = System.nanoTime();
    }

    public void acquire() {
        long now = elapsedNanos + System.nanoTime() - lastAcquireNanoSec;
        long waitTime = getWaitTime(now);
        try {
            TimeUnit.NANOSECONDS.sleep(waitTime);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        elapsedNanos = now;
        lastAcquireNanoSec = System.nanoTime();
    }

    private synchronized long getWaitTime(long now) {
        return Math.max(getEarliestAvailableTime(now) - now, 0);
    }

    private long getEarliestAvailableTime(long now) {
        reSync(now);
        long res = nextFreeTime;

        double spend = Math.min(1, storedPermits);
        double freshPermits = 1 - spend;
        long waitTime = (long) (freshPermits * grantInterval / pacingGain);
        this.nextFreeTime = saturatedAdd(nextFreeTime, waitTime);
        --storedPermits;

        return res;
    }

    private long saturatedAdd(long a, long b) {
        long naiveSum = a + b;
        return (a ^ b) < 0L | (a ^ naiveSum) >= 0L ? naiveSum : 9223372036854775807L + (naiveSum >>> 63 ^ 1L);
    }

    private void reSync(long now) {
        if (now > nextFreeTime) {
            double newPermits = (now - nextFreeTime) / grantInterval;
            storedPermits += newPermits;
            nextFreeTime = now;
        }
    }

    public void setRate(double computingRate) {
        this.grantInterval =  1e6 / computingRate;
    }
}
