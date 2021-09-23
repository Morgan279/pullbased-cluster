package com.aliware.tianchi.processor;

import com.aliware.tianchi.constant.Config;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLimitProcessor {

    private final static Logger logger = LoggerFactory.getLogger(ConcurrentLimitProcessor.class);

    private static final long RW = Config.RT_TIME_WINDOW;

    private static final int CW_FACTOR = Config.COMPUTING_RATE_WINDOW_FACTOR;

    private static final double[] GAIN_VALUES = {1.04, 0.99, 1, 1, 1, 1, 1, 1};

    private final Object UPDATE_LOCK = new Object();

    private volatile ConcurrentLimitStatus status;

    private volatile double gain;

    private volatile long lastPhaseStartedTime;

    private long lastSamplingTime;

    private volatile double RTPropEstimated;

    private volatile double computingRateEstimate;

    private final AtomicInteger roundCounter;

    private volatile boolean congestion;

    private final int threads;

    public ConcurrentLimitProcessor(int threads) {
        this.gain = 2 / Math.log(2);
        this.threads = threads;
        this.status = ConcurrentLimitStatus.PROBE;
        this.roundCounter = new AtomicInteger(0);
        this.congestion = false;
        this.RTPropEstimated = threads / 1000d;
        this.computingRateEstimate = threads;
        this.lastSamplingTime = System.currentTimeMillis();
        this.lastPhaseStartedTime = System.currentTimeMillis();
        this.initSchedule();
    }


    public int getInflightBound() {
        return (int) (gain * computingRateEstimate * RTPropEstimated * threads * 32);
    }


    public void onACK(double RTT, long averageRT, double computingRate) {
        switch (status) {
            case PROBE:
                this.handleProbe(RTT, averageRT, computingRate);
                return;

            case DRAIN:
                this.handleDrain(computingRate);
        }
    }

    private void handleProbe(double RTT, long averageRT, double computingRate) {
        long now = System.currentTimeMillis();

        if (now - lastPhaseStartedTime > RTPropEstimated) {
            gain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length];
            lastPhaseStartedTime = now;
        }

        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
            now = System.currentTimeMillis();
            if (now - lastSamplingTime > CW_FACTOR * averageRT && computingRate > computingRateEstimate) {
                computingRateEstimate = computingRate;
                congestion = false;
                lastSamplingTime = now;
            }
        }

    }


    private void handleDrain(double computingRate) {
        synchronized (UPDATE_LOCK) {
            computingRateEstimate = Math.max(computingRateEstimate, computingRate);
        }
    }

    private void initSchedule() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("time-window", true));

        scheduledExecutorService.scheduleAtFixedRate(() -> RTPropEstimated = Config.RT_PROP_ESTIMATE_VALUE, RW, RW, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (congestion) {
                this.gain = Config.DRAIN_GAIN;
                this.status = ConcurrentLimitStatus.DRAIN;

                scheduledExecutorService.schedule(() -> {
                    int round;
                    do {
                        round = ThreadLocalRandom.current().nextInt(GAIN_VALUES.length);
                    } while (round == 1);
                    roundCounter.set(round);

                    this.congestion = true;
                    this.status = ConcurrentLimitStatus.PROBE;
                }, Config.DRAIN_INTERVAL, TimeUnit.MILLISECONDS);

            }
        }, Config.CONGESTION_SCAN_INTERVAL * 10, Config.CONGESTION_SCAN_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private enum ConcurrentLimitStatus {
        DRAIN,
        PROBE;
    }
}
