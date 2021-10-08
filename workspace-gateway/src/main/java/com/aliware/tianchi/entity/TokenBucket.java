package com.aliware.tianchi.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TokenBucket {

    private final static Logger LOGGER = LoggerFactory.getLogger(TokenBucket.class);

    private double storedPermits;

    private volatile double grantInterval;

    private long elapsedMicroSec;

    private long lastAcquireMicroSec;

    private long nextFreeTime;

    public volatile double pacingGain;

    public long nextSendTime;

    private double computingRate;

    public TokenBucket(double computingRate, double pacingGain) {
        this.setRate(computingRate);
        this.computingRate = computingRate;
        this.pacingGain = pacingGain;
        this.storedPermits = 0D;
        this.elapsedMicroSec = 0L;
        this.nextFreeTime = 0L;
        this.nextSendTime = 0L;
        this.lastAcquireMicroSec = System.nanoTime();
    }

    private volatile boolean isSent = false;

//    private int probe = 1;

    public void send(AtomicInteger waiting, double RTPropEstimated, int port) {
        if (isSent) return;
        isSent = true;
        long now = (long) (elapsedMicroSec + (System.nanoTime() - lastAcquireMicroSec) / 1e3);
//        int waitingNum = waiting.get() + 1;
//        double interval = waitingNum / (pacingGain * (computingRate / 1e3));
//        synchronized (this) {
//            if (ThreadLocalRandom.current().nextDouble() < 0.0024 / RTPropEstimated) {
//                interval = 1 / (Math.sqrt(interval) + 1);
////                LOGGER.info("interval: {}", interval);
//            }
//            nextSendTime = (long) (now + waitingNum / interval);
////            LOGGER.info("nextSendTime: {} interval: {}", nextSendTime, interval);
//        }
        nextSendTime = (long) (now + waiting.get() / (pacingGain * (computingRate / 1e3)));
        LOGGER.info("{}port#?{}#?{}#?{}#?{}", port, now, RTPropEstimated, nextSendTime - now, waiting.get());
        waiting.set(0);
        lastAcquireMicroSec = System.nanoTime();
        elapsedMicroSec = now;
//        probe = 1;
        isSent = false;
    }

    public boolean canSend() {
        long now = (long) (elapsedMicroSec + (System.nanoTime() - lastAcquireMicroSec) / 1e3);
        return now >= nextSendTime;
    }

    public void acquire() {
        long now = elapsedMicroSec + System.nanoTime() - lastAcquireMicroSec;
        long waitTime = getWaitTime(now);
        try {
            TimeUnit.NANOSECONDS.sleep(waitTime);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        elapsedMicroSec = now;
        lastAcquireMicroSec = System.nanoTime();
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
