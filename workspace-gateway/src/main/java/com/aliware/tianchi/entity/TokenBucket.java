package com.aliware.tianchi.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TokenBucket {

    private final static Logger LOGGER = LoggerFactory.getLogger(TokenBucket.class);

    private double storedPermits;

    private volatile double grantInterval;

    private long elapsedNanos;

    private long lastAcquireNanoSec;

    private long nextFreeTime;

    public volatile double pacingGain;

    public long nextSendTime = 0;

    private double computingRate;

    public TokenBucket(double computingRate, double pacingGain) {
        this.setRate(computingRate);
        this.computingRate = computingRate;
        this.pacingGain = pacingGain;
        this.storedPermits = 0D;
        this.elapsedNanos = 0L;
        this.nextFreeTime = 0L;
        this.lastAcquireNanoSec = System.nanoTime();
    }

    private volatile boolean isSent = false;

    public void send(long sendTime, AtomicInteger waiting, double curComputingRate, int port) {
        if (isSent) return;
        isSent = true;
        long now = (long) (elapsedNanos + (sendTime - lastAcquireNanoSec) / 1e3);
        long interval = (long) (waiting.get() / (pacingGain * (curComputingRate / 1e3)));
        LOGGER.info("{}port#?{}#?{}#?{}#?{}#?{}#?{}", port, now, interval, computingRate, curComputingRate, waiting.get(), pacingGain);
        nextSendTime = (long) (now + waiting.get() / (pacingGain * (computingRate / 1e3)));
        //waiting.set(0);
        elapsedNanos = now;
        lastAcquireNanoSec = System.nanoTime();
        isSent = false;
    }

    public boolean canSend() {
        long now = (long) (elapsedNanos + (System.nanoTime() - lastAcquireNanoSec) / 1e3);
        return now >= nextSendTime;
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
        this.nextFreeTime = nextFreeTime + waitTime;
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
        this.computingRate = computingRate;
        this.grantInterval = 1e6 / computingRate;
    }
}
