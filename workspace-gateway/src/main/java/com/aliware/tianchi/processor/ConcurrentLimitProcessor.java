package com.aliware.tianchi.processor;

import com.aliware.tianchi.constant.Config;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLimitProcessor {

    private final static Logger logger = LoggerFactory.getLogger(ConcurrentLimitProcessor.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("time-window", true));

    private static final long RW = Config.RT_TIME_WINDOW;

    private static final int CW_FACTOR = 6;

    private static final double[] GAIN_VALUES = {1.25, 0.75, 1, 1, 1, 1, 1, 1};

    private final Object UPDATE_LOCK = new Object();

    private volatile ConcurrentLimitStatus status;

    private volatile double gain;

    private volatile long lastPhaseStartedTime;

    private long lastSamplingTime;

    public volatile double RTPropEstimated;

    private volatile double lastRTPropEstimated;

    public volatile double computingRateEstimate;

    public ConcurrentLinkedQueue<Boolean> funnel;

    private final ScheduledExecutorService funnelScheduler = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("funnel-timer", true));

    private final AtomicInteger roundCounter;

    private volatile boolean congestion;

    private volatile boolean init = false;

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
        this.lastPhaseStartedTime = System.currentTimeMillis();
        this.funnel = new ConcurrentLinkedQueue<>();
        this.initSchedule();
    }

    private int getLeakingRate() {
        return (int) (1000 / gain / computingRateEstimate);
    }

    private class Leaking implements Runnable {

        @Override
        public void run() {
//            if (funnel.size() < threads * 100) {
//                funnel.add(true);
//            }
            funnel.add(true);
            funnelScheduler.schedule(this, getLeakingRate(), TimeUnit.MICROSECONDS);
            //logger.info("interval: {}", (int) (1000 / gain / computingRateEstimate));
        }
    }


    public int getInflightBound() {
        //logger.info("computingRateEstimate: {}", (int) (computingRateEstimate * RTPropEstimated));
        //return ConcurrentLimitStatus.FILL_UP.equals(this.status) ? Integer.MAX_VALUE : 1200;
        return (int) Math.max(gain * Math.pow(computingRateEstimate, 2) * RTPropEstimated * threads * 16, 8d * threads);
        //return (int) (gain * computingRateEstimate * computingRateEstimate * RTPropEstimated * threads);
        //return (int) (computingRateEstimate);
    }


    public void onACK(double RTT, long averageRT, double computingRate, double comingRate) {
        lastRTPropEstimated = RTT;
        switch (status) {
            case PROBE:
                this.handleProbe(RTT, averageRT, computingRate);
                return;

            case FILL_UP:
                this.handleFillUp(RTT, computingRate);
                return;

            case DRAIN:
                this.handleDrain(computingRate);
        }

    }

    public void switchDrain() {
        if (ConcurrentLimitStatus.DRAIN.equals(this.status)) return;

        this.status = ConcurrentLimitStatus.DRAIN;
        this.gain = (Math.log(2) / 2);


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
        this.gain = (2 / Math.log(2));

        scheduledExecutorService.schedule(() -> {
            roundCounter.set(1);
            this.status = ConcurrentLimitStatus.PROBE;
        }, 4, TimeUnit.MILLISECONDS);
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
            if (now - lastSamplingTime > CW_FACTOR * averageRT) {
                if (computingRate > computingRateEstimate) {
                    congestion = false;
                }
                computingRateEstimate = computingRate;
                lastSamplingTime = now;
            } else {
                computingRateEstimate = Math.max(computingRateEstimate, computingRate);
            }
        }

    }

    private void handleFillUp(double RTT, double computingRate) {
//        RTPropEstimated = Math.min(RTPropEstimated, RTT);
        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
            //computingRateEstimate = Math.max(computingRateEstimate, computingRate);
        }
    }

    private void handleDrain(double computingRate) {
//        computingRateEstimate = Math.max(computingRateEstimate, computingRate);
        synchronized (UPDATE_LOCK) {
            computingRateEstimate = Math.max(computingRateEstimate, computingRate);
        }
    }

    public void initSchedule() {
        funnelScheduler.schedule(new Leaking(), 1, TimeUnit.SECONDS);
        scheduledExecutorService.schedule(() -> this.status = ConcurrentLimitStatus.PROBE, 1000, TimeUnit.MILLISECONDS);
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
