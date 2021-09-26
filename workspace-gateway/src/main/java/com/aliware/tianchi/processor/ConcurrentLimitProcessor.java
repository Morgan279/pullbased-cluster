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

    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("time-window", true));

    private static final long RW = Config.RT_TIME_WINDOW;

    private static final int CW_FACTOR = 4;

    private static final double[] GAIN_VALUES = {125, 75, 100, 100, 100, 100, 100, 100};

    private final Object UPDATE_LOCK = new Object();

    private volatile ConcurrentLimitStatus status;

    private volatile double gain;

    private volatile long lastPhaseStartedTime;

    private long lastSamplingTime;

    private volatile double RTPropEstimated;

    private volatile double lastRTPropEstimated;

    private volatile double computingRateEstimate;

    private final AtomicInteger roundCounter;

    private volatile boolean congestion;

    private final int threads;

    public ConcurrentLimitProcessor(int threads) {
        this.gain = 2 / Math.log(2);
        this.threads = threads;
        this.status = ConcurrentLimitStatus.FILL_UP;
        this.roundCounter = new AtomicInteger(0);
        this.congestion = false;
        this.RTPropEstimated = threads / 1000d;
        this.lastRTPropEstimated = RTPropEstimated;
        this.computingRateEstimate = threads;
        this.lastSamplingTime = System.currentTimeMillis();
        this.lastPhaseStartedTime = System.nanoTime();
        this.initSchedule();
    }


    public int getInflightBound() {
        return (int) (gain * 0.8 * computingRateEstimate * RTPropEstimated * threads);
    }


    public void onACK(double RTT, long averageRT, double computingRate) {
        lastRTPropEstimated = RTT;
        switch (status) {
            case PROBE:
                this.handleProbe(RTT, averageRT, computingRate);
                return;

            case FILL_UP:
                this.handleFillUp(RTT);
                return;

            case DRAIN:
                this.handleDrain(computingRate);
        }
    }

    public void switchDrain() {
        if (ConcurrentLimitStatus.DRAIN.equals(this.status)) return;

        this.status = ConcurrentLimitStatus.DRAIN;
        this.gain = (Math.log(2) / 2) * 100;


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

    public void switchFillUp() {
        if (ConcurrentLimitStatus.FILL_UP.equals(this.status)) return;

        this.status = ConcurrentLimitStatus.FILL_UP;
        this.gain = (2 / Math.log(2)) * 100;

        scheduledExecutorService.schedule(() -> {
            roundCounter.set(1);
            this.status = ConcurrentLimitStatus.PROBE;
        }, 2, TimeUnit.MILLISECONDS);
    }

    private void handleProbe(double RTT, long averageRT, double computingRate) {
        long now = System.nanoTime();

        if ((now - lastPhaseStartedTime) / 1e6 > 1.5 * RTPropEstimated) {
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

    private void handleFillUp(double RTT) {
//        RTPropEstimated = Math.min(RTPropEstimated, RTT);
        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
        }
    }

    private void handleDrain(double computingRate) {
//        computingRateEstimate = Math.max(computingRateEstimate, computingRate);
        synchronized (UPDATE_LOCK) {
            computingRateEstimate = Math.max(computingRateEstimate, computingRate);
        }
    }

    private void initSchedule() {
        scheduledExecutorService.schedule(() -> this.status = ConcurrentLimitStatus.PROBE, 10, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (ConcurrentLimitStatus.PROBE.equals(this.status)) {
                RTPropEstimated = lastRTPropEstimated;
            }
        }, RW, RW, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (congestion) {
                this.switchDrain();
            }
        }, Config.CONGESTION_SCAN_INTERVAL * 10, Config.CONGESTION_SCAN_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private enum ConcurrentLimitStatus {
        DRAIN,
        FILL_UP,
        PROBE;
    }
}
