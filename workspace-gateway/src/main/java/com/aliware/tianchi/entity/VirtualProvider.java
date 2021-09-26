package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.processor.ConcurrentLimitProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private final static Logger logger = LoggerFactory.getLogger(VirtualProvider.class);

    public final int threads;

    public volatile long averageRTT;

    public final AtomicInteger computed;

    public final AtomicInteger inflight;

    private final int SAMPLING_COUNT;

    private final int port;

    private final ConcurrentLimitProcessor concurrentLimitProcessor;

    private int counter;

    private long sum;

    public VirtualProvider(int port, int threads) {
        this.port = port;
        this.threads = threads;
        this.sum = 0;
        this.counter = 0;
        this.SAMPLING_COUNT = Config.SAMPLING_COUNT;
        this.averageRTT = Config.INITIAL_AVERAGE_RTT;
        this.computed = new AtomicInteger(0);
        this.inflight = new AtomicInteger(0);
        this.concurrentLimitProcessor = new ConcurrentLimitProcessor(threads);
    }

    public long getLatencyThreshold() {
        return Math.max((long) (this.averageRTT * 0.8), 5);
    }

    public boolean isConcurrentLimited() {
        return inflight.get() > concurrentLimitProcessor.getInflightBound();
    }

    public void onComputed(long latency, int lastComputed) {
        double RTT = latency / 1e6;
        double computingRate = (computed.incrementAndGet() - lastComputed) / RTT;
        if (RTT < 1) {
            this.concurrentLimitProcessor.switchFillUp();
        }
        this.concurrentLimitProcessor.onACK(RTT, this.averageRTT, computingRate);
        this.recordLatency(latency / (int) 1e6);
    }

    private synchronized void recordLatency(long latency) {
        sum += latency;
        ++counter;
        if (counter == SAMPLING_COUNT) {
            averageRTT = sum / counter;
            sum = 0;
            counter = 0;
        }
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
