package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.constant.ProviderStatus;
import com.aliware.tianchi.processor.ConcurrentLimitProcessor;
import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private final static Logger logger = LoggerFactory.getLogger(VirtualProvider.class);

    public final AtomicInteger computed;

    public int threads;

    private final int N;

    private final int port;

    public volatile ProviderStatus status;

    private volatile double threadFactor;

    private volatile int concurrent;

    private volatile int remainThreadCount;

    private final Deque<Long> errorStamp;

    private final AtomicInteger imperium;

    private int counter;

    private long sum;

    public volatile long averageRTT;

    private double initialLambda;

    private double currentLambda;

    public AtomicInteger inflight = new AtomicInteger();

    private final ConcurrentLimitProcessor concurrentLimitProcessor;

//    private final double SAMPLE_FACTOR = 0.99;
//
//    private final int queueLength = (int) (Config.SAMPLING_COUNT * (1 - SAMPLE_FACTOR));
//
//    private final PriorityQueue<Long> p99Latency = new PriorityQueue<>(queueLength, Long::compare);

    public VirtualProvider(int port, int threads) {
        this.port = port;
        this.threads = threads;
        this.remainThreadCount = threads;
        this.concurrent = threads;
        this.threadFactor = threads / 10d;
        this.N = Config.SAMPLING_COUNT;
        this.averageRTT = Config.INITIAL_AVERAGE_RTT;
        this.computed = new AtomicInteger(0);
        this.errorStamp = new ArrayDeque<>();
        this.imperium = new AtomicInteger();
        this.concurrentLimitProcessor = new ConcurrentLimitProcessor(threads);
        this.init();
    }

    private void init() {
        this.sum = 0;
        this.counter = 0;
        this.initialLambda = 0;
        this.currentLambda = 0.015;
        this.status = ProviderStatus.AVAILABLE;
    }

//    public long getP999Latency() {
//        return Optional.ofNullable(p99Latency.peek()).orElse(100L);
//    }
//
//    public double getThreadFactor(){
//        return this.threadFactor;
//    }
//
//    public double getRandomWeight(){
//        return ThreadLocalRandom.current().nextDouble(1);
//    }
//
//    public double getFactor() {
//        return (double) (threads - concurrent) / threads;
//    }
//
//    public int getConcurrent() {
//        return this.concurrent;
//    }

    public long getLatencyThreshold() {
        return Math.max((long) (this.averageRTT * 1.5), 8);
    }

    public boolean tryRequireConcurrent() {
        //return inflight.get() < 5000;
        //return true;
        //       logger.info("bound: {}", concurrentLimitProcessor.getInflightBound());
        return inflight.get() < concurrentLimitProcessor.getInflightBound();
    }

    public boolean isConcurrentLimited() {
        return inflight.get() > concurrentLimitProcessor.getInflightBound();
    }

    public void onComputed(long latency, int lastComputed) {
        double RTT = latency / 1e6;
        double computingRate = (computed.incrementAndGet() - lastComputed) / RTT;
        this.concurrentLimitProcessor.handleProbe(RTT, this.averageRTT, computingRate);
        this.recordLatency(latency / (int) 1e6);

    }

    public boolean hasImperium() {
        return imperium.get() > 0;
    }


    public void executeImperium() {
        imperium.decrementAndGet();
    }

    public double getRTWeight() {
        return currentLambda - initialLambda;
    }

    public double getWeight() {
        double lambdaDiff = currentLambda - initialLambda;
        double RTWeight = lambdaDiff > 0 ? lambdaDiff * 10 : 1;
        double weight = RTWeight + ThreadLocalRandom.current().nextDouble(Math.abs(Supervisor.maxWeight - RTWeight) * 1.414);
        Supervisor.maxWeight = Math.max(Supervisor.maxWeight, weight);
        return weight;
    }

    public void recordLatency(long latency) {
//        for (long i = IMPERIUM_BOUND - latency; i >= 0; --i) {
//            imperium.incrementAndGet();
//        }
        synchronized (this) {
            sum += latency;
            ++counter;
            if (counter == N) {
//                refreshLambda();
                averageRTT = sum / counter;
                sum = 0;
                counter = 0;
            }
//            if (p99Latency.size() < queueLength) {
//                p99Latency.add(latency);
//            } else if (latency > p99Latency.peek()) {
//                p99Latency.poll();
//                p99Latency.add(latency);
//            }
        }
    }


    public synchronized void recordError() {
        long now = System.currentTimeMillis();
        while (!errorStamp.isEmpty() && (now - errorStamp.getFirst() > 5)) {
            errorStamp.removeFirst();
        }
        errorStamp.addLast(now);
    }


    private void refreshLambda() {
        double value = (double) N / sum;
        if (initialLambda == 0) {
            initialLambda = value;
        }
        currentLambda = value;
        //System.out.println("currentLambda: " + currentLambda + " initial: " + initialLambda + " diff: " + (currentLambda - initialLambda));
    }

    public void setConcurrent(int concurrent) {
        if (concurrent == 0) {
            RoundRobinProcessor.reset();
        } else if (concurrent < 30) {
            imperium.set(imperium.get() + 30 - concurrent);
        }
        this.concurrent = concurrent;
    }

    public int getPort() {
        return this.port;
    }

    public int getRemainThreadCount() {
        return this.remainThreadCount;
    }

    public void setRemainThreadCount(int remainThreadCount) {
        this.remainThreadCount = remainThreadCount;
    }

    public void setThreadFactor(double threadFactor) {
        this.threadFactor = threadFactor;
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
