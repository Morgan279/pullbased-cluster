package com.aliware.tianchi.processor;

import com.aliware.tianchi.constant.Config;
import io.netty.util.internal.ThreadLocalRandom;
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

    private static final long RW = Config.RT_TIME_WINDOW;

    private static final int CW_FACTOR = 6;

    private static final double[] GAIN_VALUES = {1.25, 0.75, 1, 1, 1, 1, 1, 1};

    private final Object UPDATE_LOCK = new Object();

    private volatile ConcurrentLimitStatus status;

    private volatile double gain;

    private volatile long lastPhaseStartedTime;

    private long lastSamplingTime;

    public volatile double RTPropEstimated;

    public volatile double lastRTPropEstimated;

    public volatile double computingRateEstimated;

    private volatile double lastComputingRateEstimated;

    private final AtomicInteger roundCounter;


    private volatile boolean congestion;

    private volatile double sum = 0;

    private volatile int counter = 0;

    private volatile double RTSum = 0;

    private volatile int RTCounter = 0;

    private int round = 0;

    private final int threads;

    public ConcurrentLimitProcessor(int threads) {
        this.threads = threads;
        this.gain = 2 / Math.log(2);
        this.status = ConcurrentLimitStatus.PROBE;
        this.roundCounter = new AtomicInteger(0);
        this.congestion = false;
        this.RTPropEstimated = threads / 500D;
        this.lastRTPropEstimated = RTPropEstimated;
        this.computingRateEstimated = threads;
        this.lastComputingRateEstimated = computingRateEstimated;
        this.lastSamplingTime = System.currentTimeMillis();
        this.lastPhaseStartedTime = System.currentTimeMillis();
        this.initSchedule();
    }


    public int getInflightBound() {
//        if (ThreadLocalRandom.current().nextDouble() < 0.00024 / lastRTPropEstimated) {
//            RTPropEstimated = lastRTPropEstimated * 1.5;
//            //System.out.println((int) (gain * computingRateEstimated * RTPropEstimated));
//        }
        return (int) (gain * computingRateEstimated * RTPropEstimated);
    }


    private void initSchedule() {
        scheduledExecutorService.schedule(new GainUpdater(), 100, TimeUnit.MILLISECONDS);
        scheduledExecutorService.schedule(new SampleUpdater(), 100, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            RTPropEstimated = RTSum / Math.max(1, RTCounter);
            RTSum = RTCounter = 0;
        }, 100, 100, TimeUnit.MILLISECONDS);
        //funnelScheduler.schedule(new Leaking(), 1L, TimeUnit.SECONDS);
        //scheduledExecutorService.schedule(() -> this.status = ConcurrentLimitStatus.PROBE, 4000, TimeUnit.MILLISECONDS);
//        scheduledExecutorService.scheduleAtFixedRate(() -> {
//            gain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length];
////            if (ConcurrentLimitStatus.PROBE.equals(this.status)) {
////                gain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length];
////            }
//        }, 1000, 1250, TimeUnit.MICROSECONDS);

//        scheduledExecutorService.scheduleAtFixedRate(() -> {
//            RTPropEstimated = lastRTPropEstimated;
//            computingRateEstimated = lastComputingRateEstimated;
////            if (ConcurrentLimitStatus.PROBE.equals(this.status)) {
////                computingRateEstimated = lastComputingRateEstimated;
////                RTPropEstimated = lastRTPropEstimated;
////            }
//        }, 1000, 10, TimeUnit.MILLISECONDS);

    }

    private class GainUpdater implements Runnable {

        @Override
        public void run() {
            gain = GAIN_VALUES[round++ % GAIN_VALUES.length];
            scheduledExecutorService.schedule(this, Math.round(RTPropEstimated * 1e3), TimeUnit.MICROSECONDS);
        }
    }

    private class SampleUpdater implements Runnable {

        @Override
        public void run() {
            //RTPropEstimated = lastRTPropEstimated;
            computingRateEstimated = sum / Math.max(counter, 1);
            sum = counter = 0;
            scheduledExecutorService.schedule(this, Math.round(10 * RTPropEstimated * 1e3), TimeUnit.MICROSECONDS);
        }
    }

    public void handleProbe(double RTT, double computingRate) {
        lastRTPropEstimated = RTT;
        lastComputingRateEstimated = computingRate;
        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
            computingRateEstimated = Math.max(computingRateEstimated, computingRate);
            sum += computingRate;
            ++counter;
            RTSum += RTT;
            ++RTCounter;
        }

    }

    public void onACK(double RTT, double computingRate) {
        lastRTPropEstimated = RTT;
        lastComputingRateEstimated = computingRate;
        switch (status) {
            case PROBE:
                this.handleProbe(RTT, computingRate);
                break;

            case FILL_UP:
                this.handleFillUp(RTT, computingRate);
                break;

            case DRAIN:
                this.handleDrain(computingRate);
        }

    }

    public void switchDrain() {
        if (isDraining()) return;

        this.status = ConcurrentLimitStatus.DRAIN;
        gain = 0.5;


        scheduledExecutorService.schedule(() -> {
            int round;
            do {
                round = ThreadLocalRandom.current().nextInt(GAIN_VALUES.length);
            } while (round == 1);
            roundCounter.set(round);

            this.congestion = true;
            this.status = ConcurrentLimitStatus.PROBE;
        }, 4, TimeUnit.MILLISECONDS);
    }

    public void switchFillUp() {
        if (ConcurrentLimitStatus.FILL_UP.equals(this.status)) return;

        this.status = ConcurrentLimitStatus.FILL_UP;
        gain = 2;

        scheduledExecutorService.schedule(() -> {
            roundCounter.set(1);
            this.status = ConcurrentLimitStatus.PROBE;
        }, 1, TimeUnit.MILLISECONDS);
    }


    private void handleFillUp(double RTT, double computingRate) {
//        RTPropEstimated = Math.min(RTPropEstimated, RTT);
        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
            //computingRateEstimated = Math.max(computingRateEstimated, computingRate);
        }
    }

    private void handleDrain(double computingRate) {
//        computingRateEstimated = Math.max(computingRateEstimated, computingRate);
        synchronized (UPDATE_LOCK) {
            computingRateEstimated = Math.max(computingRateEstimated, computingRate);
        }
    }


    public boolean isDraining() {
        return ConcurrentLimitStatus.DRAIN.equals(this.status);
    }

    private enum ConcurrentLimitStatus {
        DRAIN,
        FILL_UP,
        PROBE;
    }
}
