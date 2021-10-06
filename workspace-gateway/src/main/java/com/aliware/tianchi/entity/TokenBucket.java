package com.aliware.tianchi.entity;

import io.netty.util.internal.ThreadLocalRandom;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TokenBucket {

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

    private int probe = 1;

    public void send(AtomicInteger waiting, double RTPropEstimated) {
        if (isSent) return;
        isSent = true;
        long now = (long) (elapsedNanos + (System.nanoTime() - lastAcquireNanoSec) / 1e3);
        synchronized (this) {
            if (ThreadLocalRandom.current().nextDouble() < 0.003 / RTPropEstimated) {
                probe = 32;
            }
            nextSendTime = (long) (now + waiting.get() / (pacingGain * probe * (computingRate / 1e3)));
        }
        //System.out.println("now: " + now + " nextSendTime: " + nextSendTime + "  waiting: " + waiting.get());
        waiting.set(0);
        lastAcquireNanoSec = System.nanoTime();
        elapsedNanos = now;
        probe = 1;
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
