package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.processor.ConcurrentLimitProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(VirtualProvider.class);

    //private final ScheduledExecutorService scheduledExecutorService;

    public final int threads;

    public volatile long averageRTT;

    public final AtomicInteger computed;

    public final AtomicInteger inflight;

    public final AtomicInteger assigned;

    public final AtomicInteger error;

    public final AtomicInteger comingNum;

    public final AtomicInteger waiting;

    private final int SAMPLING_COUNT;

    private final int port;

    public final ConcurrentLimitProcessor concurrentLimitProcessor;

    private int counter;

    private long sum;

    private volatile long lastSamplingTime = System.currentTimeMillis();

    public long recentMaxLatency = 0;

    public VirtualProvider(int port, int threads) {
        this.port = port;
        this.threads = threads;
        this.sum = 0;
        this.counter = 0;
        this.SAMPLING_COUNT = Config.SAMPLING_COUNT;
        this.averageRTT = Config.INITIAL_AVERAGE_RTT;
        this.computed = new AtomicInteger(0);
        this.inflight = new AtomicInteger(0);
        this.assigned = new AtomicInteger(1);
        this.error = new AtomicInteger(0);
        this.comingNum = new AtomicInteger(0);
        this.waiting = new AtomicInteger(0);
        this.concurrentLimitProcessor = new ConcurrentLimitProcessor(threads);
//        for (int i = 0, len = (int) (threads * 0.8); i < len; ++i) {
//            Supervisor.workLoads.add(new WorkLoad(port, 2 + ThreadLocalRandom.current().nextDouble()));
//        }
        //scheduledExecutorService = Executors.newScheduledThreadPool(threads / 3, new NamedInternalThreadFactory("concurrent-timer", true));
    }

    public long getLatencyThreshold() {
        return (long) (Math.max(concurrentLimitProcessor.RTPropEstimated, 1) * ThreadLocalRandom.current().nextDouble(1.5, 3));
        //return Math.max((long) (this.averageRTT * 1.1), 7);
    }

    public boolean isConcurrentLimited() {
        //return concurrentLimitProcessor.rateLimiter.tryAcquire();
        //LOGGER.info("inflight: {} computing rate: {}", inflight.get(), concurrentLimitProcessor.computingRateEstimated);
        return inflight.get() > concurrentLimitProcessor.getInflightBound();
    }

    public double getErrorRatio() {
        //logger.info("assigned: {} error: {} ratio: {}", assigned.get(), error.get(), (double) error.get() / assigned.get() / 3);
        return Math.pow(error.get() * 100D / assigned.get(), 0.8D) / 100;
    }

    public void onComputed(long latency, int lastComputed) {
        double RTT = latency / 1e6;
//        if (RTT < 5) {
//            for (int i = 0; i < 3; ++i) {
//                Supervisor.workLoads.pollLast();
//                Supervisor.workLoads.add(new WorkLoad(port, ThreadLocalRandom.current().nextDouble()));
//            }
//            //concurrentLimitProcessor.switchFillUp();
//        }
//        Supervisor.workLoads.add(new WorkLoad(port, RTT));
        double computingRate = (computed.get() - lastComputed) / RTT;
//        LOGGER.info("avg: {}", averageRTT);
        this.concurrentLimitProcessor.onACK(RTT, computingRate);
        LOGGER.info("{}port#?{}#?{}#?{}#?{}#?{}", port, RTT, computingRate, inflight.get(), concurrentLimitProcessor.getInflightBound(), waiting.get());
        //this.recordLatency(latency / (int) 1e6);
    }

    public void refreshErrorSampling() {
        long now = System.currentTimeMillis();
        if (now - lastSamplingTime > 100 * concurrentLimitProcessor.RTPropEstimated) {
            assigned.set(1);
            error.set(0);
            lastSamplingTime = now;
        }
    }

    private synchronized void recordLatency(long latency) {
        counter = (counter + 1) % 10;
        if (counter == 9) {
            recentMaxLatency = latency;
        } else {
            recentMaxLatency = Math.max(recentMaxLatency, latency);
        }
//        sum += latency;
//        ++counter;
//        if (counter == SAMPLING_COUNT) {
//            averageRTT = sum / counter;
//            sum = 0;
//            counter = 0;
//        }
    }

    public void switchDrain() {
        this.concurrentLimitProcessor.switchDrain();
    }

    public int getPort() {
        return this.port;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VirtualProvider)) return false;
        VirtualProvider that = (VirtualProvider) o;
        return getPort() == that.getPort();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPort());
    }
}
