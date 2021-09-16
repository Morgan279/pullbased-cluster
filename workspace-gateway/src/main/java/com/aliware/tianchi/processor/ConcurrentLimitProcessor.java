package com.aliware.tianchi.processor;

import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLimitProcessor {

    private static final long WR = 10;

    private static final int WB_FACTOR = 6;

    private static final double[] GAIN_VALUES = {125, 75, 100, 100, 100, 100, 100, 100};

    private final Object UPDATE_LOCK = new Object();

    private final ScheduledExecutorService scheduledExecutorService;

    private volatile ConcurrentLimitStatus status;

    private volatile double gain;

    private volatile long lastPhaseStartedTime;

    private volatile double lastComputingRate;

    private long lastSamplingTime;

    private long RTPropEstimated;

    private double computingRateEstimate;

    public AtomicInteger roundCounter;

    private AtomicInteger plateauCounter;

    public ConcurrentLimitProcessor(int threads) {
        this.gain = 2 / Math.log(2);
        this.lastComputingRate = 0;
        this.status = ConcurrentLimitStatus.START_UP;
        this.roundCounter = new AtomicInteger(0);
        this.plateauCounter = new AtomicInteger(0);
        this.RTPropEstimated = 1000 / threads;
        this.computingRateEstimate = threads;
        this.lastSamplingTime = System.currentTimeMillis();
        this.lastPhaseStartedTime = System.currentTimeMillis();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("sampling-timer", true));
        //this.scheduledExecutorService.scheduleAtFixedRate(() -> RTPropEstimated = 100, WR, WR, TimeUnit.MILLISECONDS);
    }

    public void onACK(long RTT, long averageRT, double computingRate) {
        switch (status) {
            case PROBE:
                this.handleProbe(RTT, averageRT, computingRate);
                return;

            case START_UP:
                this.handleStartup(computingRate);
                return;

            case DRAIN:
                this.handleDrain();
        }
    }

    public int getInflightBound() {
        return (int) (gain * computingRateEstimate * RTPropEstimated);
    }

    private void handleProbe(long RTT, long averageRT, double computingRate) {
        long now = System.currentTimeMillis();

        if (now - lastPhaseStartedTime >= RTPropEstimated) {
            gain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length];
            lastPhaseStartedTime = now;
        }

        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
            now = System.currentTimeMillis();
            if (now - lastSamplingTime >= WB_FACTOR * averageRT) {
                lastSamplingTime = now;
                computingRateEstimate = -1;
            }
            computingRateEstimate = Math.max(computingRateEstimate, computingRate);
        }
    }

    private void handleStartup(double computingRate) {
        if ((computingRate - lastComputingRate) / lastComputingRate < 0.25) {
            if (plateauCounter.incrementAndGet() == 3) {
                this.status = ConcurrentLimitStatus.PROBE;
                this.lastComputingRate = 0;
                this.plateauCounter.set(0);
            }
        } else {
            this.lastComputingRate = computingRate;
            this.plateauCounter.set(0);
        }
    }

    private void handleDrain() {

    }

    private enum ConcurrentLimitStatus {
        START_UP,
        DRAIN,
        PROBE;
    }
}
