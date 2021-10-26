package com.aliware.tianchi.entity;

import com.aliware.tianchi.processor.Observer;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Sampler implements Runnable {

    public static final int SAMPLE_INTERVAL = 10;

    private final static Logger LOGGER = LoggerFactory.getLogger(Sampler.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedInternalThreadFactory("sample-timer", true));

    private double lastRate = 0;

    private double currentRate = 0;

    public double avgRTT = 1;

    private double RTTSum = 0;

    public AtomicInteger RTTCount = new AtomicInteger();

    private final AtomicInteger assigned = new AtomicInteger();

    private final AtomicInteger error = new AtomicInteger();

    private final AtomicInteger computed = new AtomicInteger();

    private Observer observer;

    private volatile boolean isSampling = false;

    public void registerObserver(Observer observer) {
        this.observer = observer;
    }

    public synchronized void startSample() {
        if (isSampling) {
            return;
        }

        reset();
        this.isSampling = true;
        scheduledExecutorService.schedule(this, SAMPLE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public void onComputed(double rate, double RTT) {
        if (!isSampling) {
            return;
        }

        computed.incrementAndGet();
        RTTCount.incrementAndGet();
//        LOGGER.info("computed: {}", computed.get());
        synchronized (this) {
            RTTSum += RTT;
            //currentRate = Math.max(currentRate, rate);
        }
    }

    public void assign() {
        if (!isSampling) {
            return;
        }

        assigned.incrementAndGet();
    }

    public void onError() {
        if (!isSampling) {
            return;
        }

        error.incrementAndGet();
    }

    private double getErrorRatio() {
        if (assigned.get() == 0) {
            return 0;
        }

        return (double) error.get() / assigned.get();
    }

    private void reset() {
        assigned.set(0);
        error.set(0);
        computed.set(0);
        RTTCount.set(0);
        lastRate = currentRate;
        currentRate = RTTSum = 0;
    }


    @Override
    public void run() {
        isSampling = false;
        //currentRate = Math.max(currentRate, (double) computed.get() / SAMPLE_INTERVAL);
        currentRate = (double) computed.get() / SAMPLE_INTERVAL;
        //LOGGER.info("rate: {} es: {} last: {}", (double) computed.get() / SAMPLE_INTERVAL, currentRate, lastRate);
        double deltaRate = lastRate == 0 ? 0 : (currentRate - lastRate) / lastRate;
        if (getErrorRatio() > 0.3) {
            startSample();
        } else {
            avgRTT = RTTSum / Math.max(1, RTTCount.get());
            observer.onSampleComplete(currentRate, deltaRate);
        }
    }
}
