package com.aliware.tianchi;

import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLimitProcessor {

    private final static Logger logger = LoggerFactory.getLogger(ConcurrentLimitProcessor.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(4, new NamedInternalThreadFactory("time-window", true));

    private static final double[] GAIN_VALUES = {1.25, 0.75, 1, 1, 1, 1, 1, 1};

    private final Object UPDATE_LOCK = new Object();

    private volatile double gain;

    private volatile double RTPropEstimated;

    private volatile double lastRTPropEstimated;

    private volatile double computingRateEstimated;

    private volatile double lastComputingRateEstimated;

    private final AtomicInteger roundCounter;


    public ConcurrentLimitProcessor() {
        this.gain = 2 / Math.log(2);
        this.roundCounter = new AtomicInteger(0);
        this.RTPropEstimated = 1;
        this.lastRTPropEstimated = RTPropEstimated;
        this.computingRateEstimated = 500;
        this.lastComputingRateEstimated = computingRateEstimated;
        this.initSchedule();
    }


    public int getBound() {
        return (int) (gain * computingRateEstimated * RTPropEstimated);
    }


    public void onResponse(double RTT, double computingRate) {
        lastRTPropEstimated = RTT;
        lastComputingRateEstimated = computingRate;
        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
            computingRateEstimated = Math.max(computingRateEstimated, computingRate);
        }
    }


    private void initSchedule() {
        scheduledExecutorService.scheduleAtFixedRate(() -> gain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length], 1000, 1250, TimeUnit.MICROSECONDS);

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            RTPropEstimated = lastRTPropEstimated;
            computingRateEstimated = lastComputingRateEstimated;

        }, 1000, 10, TimeUnit.MILLISECONDS);

    }

}
