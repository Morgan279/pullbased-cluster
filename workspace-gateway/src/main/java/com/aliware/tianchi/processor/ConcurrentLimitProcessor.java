package com.aliware.tianchi.processor;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.entity.TokenBucket;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLimitProcessor {

    private final static Logger logger = LoggerFactory.getLogger(ConcurrentLimitProcessor.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(16, new NamedInternalThreadFactory("time-window", true));

    private static final long RW = Config.RT_TIME_WINDOW;

    private static final int CW_FACTOR = 6;

    private static final double[] GAIN_VALUES = {1.03, 0.99, 1, 1, 1, 1, 1, 1};

    private final Object UPDATE_LOCK = new Object();

    private volatile ConcurrentLimitStatus status;

    private volatile double gain;

    private volatile long lastPhaseStartedTime;

    private long lastSamplingTime;

    public volatile double RTPropEstimated;

    private volatile double lastRTPropEstimated;

    public volatile double lastComputingRateEstimated;

    public volatile double computingRateEstimated;

    public ConcurrentLinkedQueue<Boolean> funnel;

    //private final ScheduledExecutorService funnelScheduler = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("funnel-timer", true));

    private final AtomicInteger roundCounter;

    private volatile boolean congestion;

    private final int threads;

    public TokenBucket tokenBucket;

    public ConcurrentLimitProcessor(int threads) {
        this.threads = threads;
        this.status = ConcurrentLimitStatus.FILL_UP;
        this.roundCounter = new AtomicInteger(0);
        this.congestion = false;
        this.RTPropEstimated = threads / 1000D;
        this.lastRTPropEstimated = RTPropEstimated;
        this.computingRateEstimated = threads;
        this.lastComputingRateEstimated = computingRateEstimated;
        this.lastSamplingTime = System.currentTimeMillis();
        this.lastPhaseStartedTime = System.currentTimeMillis();
        this.tokenBucket = new TokenBucket(computingRateEstimated, 2 / Math.log(2));
        //this.funnel = new ConcurrentLinkedQueue<>();
        this.initSchedule();
    }

    private int getLeakingRate() {
        return (int) (1000 / gain / computingRateEstimated);
    }

    private class Leaking implements Runnable {

        @Override
        public void run() {
            if (ConcurrentLimitStatus.PROBE.equals(status)) {
                computingRateEstimated = lastComputingRateEstimated;
            }
            scheduledExecutorService.schedule(this, (long) (2 * RTPropEstimated), TimeUnit.MILLISECONDS);
            //funnelScheduler.schedule(this, getLeakingRate(), TimeUnit.MICROSECONDS);
        }
    }

    private class RefreshGain implements Runnable {

        @Override
        public void run() {
            if (ConcurrentLimitStatus.PROBE.equals(status)) {
                tokenBucket.pacingGain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length];
            }
            scheduledExecutorService.schedule(this, (long) (RTPropEstimated * 1e3), TimeUnit.MICROSECONDS);
        }
    }


    public int getInflightBound() {
        //logger.info("computingRateEstimated: {}", (int) (computingRateEstimated * RTPropEstimated));
        //return ConcurrentLimitStatus.FILL_UP.equals(this.status) ? Integer.MAX_VALUE : 1200;
        //return (int) Math.max(gain * Math.pow(computingRateEstimated, 2) * RTPropEstimated * threads * 16, 8d * threads);
        //return (int) (gain * computingRateEstimated * computingRateEstimated * RTPropEstimated * threads);
        return (int) (computingRateEstimated * RTPropEstimated * threads * 256);
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
                this.handleDrain(RTT, computingRate);
        }

        tokenBucket.setRate(computingRateEstimated);
    }

    public void switchDrain() {
        if (ConcurrentLimitStatus.DRAIN.equals(this.status)) return;

        this.status = ConcurrentLimitStatus.DRAIN;
//        tokenBucket.pacingGain = (Math.log(2) / 2);
        tokenBucket.pacingGain = 0.75;


        scheduledExecutorService.schedule(() -> {
            int round;
            do {
                round = ThreadLocalRandom.current().nextInt(GAIN_VALUES.length);
            } while (round == 1);
            roundCounter.set(round);

            this.congestion = true;
            this.congestionCounter.set(0);
            this.status = ConcurrentLimitStatus.PROBE;
        }, 1, TimeUnit.MILLISECONDS);
    }

    public void switchFillUp() {
        if (ConcurrentLimitStatus.FILL_UP.equals(this.status)) return;

        this.status = ConcurrentLimitStatus.FILL_UP;
        //tokenBucket.pacingGain = 2 / Math.log(2);
        tokenBucket.pacingGain = 1 / Math.log(2);

        scheduledExecutorService.schedule(() -> {
            roundCounter.set(1);
            this.status = ConcurrentLimitStatus.PROBE;
        }, 1, TimeUnit.MILLISECONDS);
    }

    private final AtomicInteger congestionCounter = new AtomicInteger(0);

    private void handleProbe(double RTT, double computingRate) {
//        long now = System.currentTimeMillis();
//
//        if (now - lastPhaseStartedTime > RTPropEstimated) {
//            tokenBucket.pacingGain = GAIN_VALUES[roundCounter.getAndIncrement() % GAIN_VALUES.length];
//            lastPhaseStartedTime = now;
//        }

        if (computingRate > computingRateEstimated) {
            congestionCounter.set(0);
            computingRateEstimated = computingRate;
        } else if (System.currentTimeMillis() - lastSamplingTime > RTPropEstimated) {
            if (congestionCounter.incrementAndGet() >= 30) {
                congestionCounter.set(0);
                this.switchDrain();
            }
            lastSamplingTime = System.currentTimeMillis();
        }

        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
//            now = System.currentTimeMillis();
//            if (now - lastSamplingTime > CW_FACTOR * averageRT) {
//                if (computingRate > computingRateEstimated) {
//                    congestion = false;
//                }
//                computingRateEstimated = computingRate;
//                lastSamplingTime = now;
//            } else {
//                computingRateEstimated = Math.max(computingRateEstimated, computingRate);
//            }
        }


    }

    private void handleFillUp(double RTT, double computingRate) {
//        if (computingRate > computingRateEstimated) {
////            computingRateEstimated = computingRate;
//            this.status = ConcurrentLimitStatus.PROBE;
//        }
        synchronized (UPDATE_LOCK) {
            //RTPropEstimated = Math.min(RTPropEstimated, RTT);
            computingRateEstimated = Math.max(computingRateEstimated, computingRate);
        }
    }

    private void handleDrain(double RTT, double computingRate) {
//        if (RTT < RTPropEstimated) {
//            this.status = ConcurrentLimitStatus.PROBE;
//        }
//        if (RTPropEstimated < RTT) {
//            RTPropEstimated = RTT;
//        }
        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
        }
    }

    public void initSchedule() {
        //funnelScheduler.schedule(new Leaking(), 1L, TimeUnit.SECONDS);
        scheduledExecutorService.schedule(() -> this.status = ConcurrentLimitStatus.PROBE, 100, TimeUnit.MILLISECONDS);
        scheduledExecutorService.schedule(new Leaking(), 100, TimeUnit.MILLISECONDS);
        scheduledExecutorService.schedule(new RefreshGain(), 1000, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (ConcurrentLimitStatus.PROBE.equals(this.status)) {
                RTPropEstimated = lastRTPropEstimated;
            }
        }, RW, RW, TimeUnit.MILLISECONDS);

//        scheduledExecutorService.scheduleAtFixedRate(() -> {
//            if (congestion) {
//                this.switchDrain();
//            }
//        }, Config.CONGESTION_SCAN_INTERVAL * 10, Config.CONGESTION_SCAN_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private enum ConcurrentLimitStatus {
        DRAIN,
        FILL_UP,
        PROBE;
    }
}
