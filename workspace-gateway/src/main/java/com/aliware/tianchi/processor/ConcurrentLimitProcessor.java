package com.aliware.tianchi.processor;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.tool.StopWatch;
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

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(8, new NamedInternalThreadFactory("time-window", true));

    private static final long RW = Config.RT_TIME_WINDOW;

    private static final int CW_FACTOR = 6;

    private static final double[] GAIN_VALUES = {1, 1, 1, 1, 1, 1, 1, 1};

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

    private final ProbeProcessor probeProcessor = new ProbeProcessor();

    private final int threads;

    public ConcurrentLimitProcessor(int threads) {
        this.threads = threads;
        this.gain = 2 / Math.log(2);
        this.status = ConcurrentLimitStatus.PROBE;
        this.roundCounter = new AtomicInteger(0);
        this.congestion = false;
        this.RTPropEstimated = 10;
        this.lastRTPropEstimated = RTPropEstimated;
        this.computingRateEstimated = 0;
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
        //return (int) (gain * computingRateEstimated * RTPropEstimated);
        return (int) (probeProcessor.bound * gain);
    }

    private final GainUpdater gainUpdater = new GainUpdater();

    private void initSchedule() {
        probeProcessor.probe();
        refreshSampling();
        scheduledExecutorService.execute(sampleUpdater);
//        scheduledExecutorService.schedule(gainUpdater, 5000, TimeUnit.MILLISECONDS);
//        scheduledExecutorService.schedule(new SampleUpdater(), 100, TimeUnit.MILLISECONDS);
//        scheduledExecutorService.scheduleAtFixedRate(() -> {
//            RTPropEstimated = RTSum / Math.max(1, RTCounter);
//            RTSum = RTCounter = 0;
//        }, 100, 10, TimeUnit.MILLISECONDS);

    }

    private class GainUpdater implements Runnable {

        @Override
        public void run() {
            //gain = GAIN_VALUES[round++ % GAIN_VALUES.length];
//            if (computingRateEstimated < 3) {
//                scheduledExecutorService.schedule(this, Math.round(RTPropEstimated * 1e3), TimeUnit.MICROSECONDS);
//                return;
//            }

            if (probeProcessor.onConverge(computingRateEstimated)) {
                logger.info("converged: {}", probeProcessor.bound);
                startCruising();
            } else {
                refreshSampling();
                scheduledExecutorService.schedule(this, 100, TimeUnit.MILLISECONDS);
            }
        }
    }

    private long getSamplingInterval() {
        //return Math.round(RTPropEstimated * 1024D);
        return probeProcessor.isDraining() ? 3 : 320;
    }

    private void refreshSampling() {
        RTPropEstimated = lastRTPropEstimated;
        lastComputingRateEstimated = computingRateEstimated;
        computingRateEstimated = sum / Math.max(counter, 1);
        sum = counter = 0;
        round = 0;
    }


    SampleUpdater sampleUpdater = new SampleUpdater();

    private class SampleUpdater implements Runnable {

        @Override
        public void run() {
            gain = probeProcessor.gains[round++ % probeProcessor.gains.length];
            if (round == probeProcessor.gains.length) {
                gain = 1;
                onConverge();
            } else {
                scheduledExecutorService.schedule(this, Math.round(RTPropEstimated * 1e3), TimeUnit.MICROSECONDS);
            }
        }
    }

    private void onConverge() {
        if (probeProcessor.onConverge(computingRateEstimated)) {
            //congestion = true;
            refreshSampling();
            stopWatch.start();
            startCruising();
        } else {
            refreshSampling();
            scheduledExecutorService.execute(sampleUpdater);
        }
    }

    private final StopWatch stopWatch = new StopWatch();

    private void startCruising() {
//        ++round;
//        if (round % 8 == 0) {
//            if (Math.abs(lastComputingRateEstimated - computingRateEstimated) / lastComputingRateEstimated > 0.05) {
//                gain = 1;
//                probeProcessor.probe();
//                refreshSampling();
//                logger.info("cruise last time: {}", stopWatch.stop());
//                scheduledExecutorService.execute(sampleUpdater);
//            } else {
//                refreshSampling();
//                scheduledExecutorService.execute(this::startCruising);
//            }
//        } else {
//            scheduledExecutorService.schedule(this::startCruising, Math.round(RTPropEstimated * 1e3), TimeUnit.MICROSECONDS);
//        }
        scheduledExecutorService.schedule(() -> {
            gain = 1;
            probeProcessor.probe();
            refreshSampling();
            scheduledExecutorService.execute(sampleUpdater);
//            congestion = false;
//            this.refreshSampling();
//            lastSamplingTime = System.currentTimeMillis() + 320;
            //scheduledExecutorService.schedule(gainUpdater, Math.round(RTPropEstimated * 1e3), TimeUnit.MICROSECONDS);
        }, 2500, TimeUnit.MILLISECONDS);
    }

    public void handleProbe(double RTT, double computingRate) {
        lastRTPropEstimated = RTT;
//        lastComputingRateEstimated = computingRate;
//        long now = System.currentTimeMillis();
//        if (!congestion && now - lastSamplingTime > getSamplingInterval()) {
//            if (probeProcessor.onConverge(computingRateEstimated)) {
//                this.congestion = true;
//                this.startCruising();
//            } else {
//                this.refreshSampling();
//            }
//            lastSamplingTime = now;
//        } else {
//            synchronized (UPDATE_LOCK) {
//                RTPropEstimated = Math.min(RTPropEstimated, RTT);
//                computingRateEstimated = Math.max(computingRateEstimated, computingRate);
//                sum += computingRate;
//                ++counter;
////            RTSum += RTT;
////            ++RTCounter;
//            }
//        }

        synchronized (UPDATE_LOCK) {
            RTPropEstimated = Math.min(RTPropEstimated, RTT);
            computingRateEstimated = Math.max(computingRateEstimated, computingRate);
            sum += computingRate;
            ++counter;
//            RTSum += RTT;
//            ++RTCounter;
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
            //computingRateEstimated = Math.max(computingRateEstimated, rate);
        }
    }

    private void handleDrain(double computingRate) {
//        computingRateEstimated = Math.max(computingRateEstimated, rate);
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
