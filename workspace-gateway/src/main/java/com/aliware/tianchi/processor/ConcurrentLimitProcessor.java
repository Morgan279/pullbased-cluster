package com.aliware.tianchi.processor;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.entity.Sampler;
import com.aliware.tianchi.tool.StopWatch;
import io.netty.util.internal.ThreadLocalRandom;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLimitProcessor implements Observer {

    private final static Logger logger = LoggerFactory.getLogger(ConcurrentLimitProcessor.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2, new NamedInternalThreadFactory("time-window", true));

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

    private volatile boolean init = false;

    private final Sampler sampler;

    public ConcurrentLimitProcessor(int threads, Sampler sampler) {
        this.threads = threads;
        this.gain = 1;
        this.status = ConcurrentLimitStatus.PROBE;
        this.roundCounter = new AtomicInteger(0);
        this.congestion = false;
        this.RTPropEstimated = 10;
        this.lastRTPropEstimated = RTPropEstimated;
        this.computingRateEstimated = 0;
        this.lastComputingRateEstimated = computingRateEstimated;
        this.lastSamplingTime = System.currentTimeMillis();
        this.lastPhaseStartedTime = System.currentTimeMillis();
        this.sampler = sampler;
        //scheduledExecutorService.schedule(sampler::startSample, 1, TimeUnit.SECONDS);
    }


    public int getInflightBound() {
        return (int) (probeProcessor.bound * gain);
    }

    private final StopWatch stopWatch = new StopWatch();

    @Override
    public void onSampleComplete(double rate, double deltaRate) {
        //logger.info("newest rate: {}", rate);
        computingRateEstimated = rate;
        switch (status) {
            case CRUISING:
                ++round;
                logger.info("Delta rate: {}", deltaRate);
                if (Math.abs(deltaRate) > 0.15) {
                    logger.info("cruise last time: {}", stopWatch.stop());
                    gain = 1;
                    round = 0;
                    probeProcessor.probe();
                    this.status = ConcurrentLimitStatus.PROBE;
                } else if (round > 2) {
                    gain *= deltaRate > 0 ? 1.1 : 0.9;
                }
                break;

            case PROBE:
                if (probeProcessor.onConverge(rate)) {
                    stopWatch.start();
                    this.status = ConcurrentLimitStatus.CRUISING;
                } else if (probeProcessor.isProbingLeft()) {
                    gain = 0.8;
                    scheduledExecutorService.schedule(() -> gain = 1, 1600, TimeUnit.MICROSECONDS);
                }
                break;

        }
        sampler.startSample();
    }


    public void handleProbe(double RTT, double computingRate) {
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
        PROBE,
        CRUISING
    }
}
