package com.aliware.tianchi.processor;

import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLimitProcessor {

    private final static Logger logger = LoggerFactory.getLogger(ConcurrentLimitProcessor.class);


    private static final long WR = 10;

    private static final int WB_FACTOR = 6;

    private static final double[] GAIN_VALUES = {1.01, 0.99, 1, 1, 1, 1, 1, 1};

    private final Object UPDATE_LOCK = new Object();

    private final ScheduledExecutorService scheduledExecutorService;

    private volatile ConcurrentLimitStatus status;

    private volatile double gain;

    private volatile long lastPhaseStartedTime;

    private volatile double lastComputingRate;

    private long lastSamplingTime;

    private volatile double RTPropEstimated;

    private volatile double computingRateEstimate;

    public AtomicInteger roundCounter;

    private AtomicInteger plateauCounter;

    private int counter;

    private int thread;

    private static volatile double throughput;

    private static volatile int bound;

    public ConcurrentLimitProcessor(int threads) {
        this.gain = 2 / Math.log(2);
        this.lastComputingRate = 0;
        this.counter = 1;
        this.throughput = 1;
        this.bound = 100;
        this.status = ConcurrentLimitStatus.PROBE;
        this.roundCounter = new AtomicInteger(0);
        this.plateauCounter = new AtomicInteger(0);
        this.RTPropEstimated = threads / 100d;
        this.computingRateEstimate = threads;
        this.thread = threads;
        this.lastSamplingTime = System.currentTimeMillis();
        this.lastPhaseStartedTime = System.currentTimeMillis();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("sampling-timer", true));
//        this.scheduledExecutorService.scheduleAtFixedRate(() -> bound += 3, 3000, 10, TimeUnit.MILLISECONDS);
        this.scheduledExecutorService.scheduleAtFixedRate(() -> RTPropEstimated = 100, WR, WR, TimeUnit.MILLISECONDS);
        //scheduledExecutorService.schedule(() -> this.status = ConcurrentLimitStatus.PROBE, 1, TimeUnit.SECONDS);
    }

//    static {
//        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("sampling-timer", true));
//        scheduledExecutorService.scheduleWithFixedDelay(() -> {
//            logger.info("throughput: {} bound: {}", throughput, bound);
//        }, 5000, 100, TimeUnit.MILLISECONDS);
//    }

    public int getInflightBound() {
        return (int) (gain * computingRateEstimate * RTPropEstimated * thread * 30);
        // return this.bound;
    }

    private long lastStaticTime = System.currentTimeMillis();

    public synchronized void onComputed(double computedDiff) {
        long now = System.currentTimeMillis();
        if (now - lastSamplingTime > 50) {
            throughput = computedDiff;
            counter = 1;
            lastStaticTime = now;
        } else {
            throughput = (throughput * counter++ + computedDiff) / counter;
        }
//        if (counter % 500 == 0) {
//            //           logger.info("throughput: {} bound: {}", throughput, bound);
//            throughput = computedDiff;
//            counter = 1;
//        } else {
//            throughput = (throughput * counter++ + computedDiff) / counter;
//        }
//        if (counter % 50 == 0) {
//            gain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length];
//        }
//        if (counter % 100 == 0) {
//            logger.info("throughput: {} bound: {}", throughput, bound);
//        }
    }

    public void onACK(double RTT, long averageRT, double computingRate) {
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

    private void handleProbe(double RTT, long averageRT, double computingRate) {
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
            if (plateauCounter.incrementAndGet() == 10) {
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
