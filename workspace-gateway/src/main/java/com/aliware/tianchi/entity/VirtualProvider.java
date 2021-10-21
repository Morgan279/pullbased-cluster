package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.constant.Factors;
import com.aliware.tianchi.processor.ConcurrentLimitProcessor;
import io.netty.util.internal.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(VirtualProvider.class);

    //private final ScheduledExecutorService scheduledExecutorService;

    public final int threads;

    private final int SAMPLING_COUNT = Config.SAMPLING_COUNT;

    public volatile double averageRTT = Config.INITIAL_AVERAGE_RTT;

    public volatile int weight = Factors.EVALUATE_FACTOR;

    public final AtomicInteger computed;

    public final AtomicInteger inflight;

    public final AtomicInteger assigned;

    public final AtomicInteger error;

    public final AtomicInteger privilege = new AtomicInteger(0);

    private final int port;

    public final ConcurrentLimitProcessor concurrentLimitProcessor;

    private int counter;

    private double sum;

    private volatile long lastSamplingTime = System.currentTimeMillis();

    public long recentMaxLatency = 0;

    public volatile int waiting = 1;

    public volatile int concurrency;

    private final Predictor predictor;

    public final Sampler sampler;

    public VirtualProvider(int port, int threads) {
        this.port = port;
        this.threads = threads;
        this.sum = 0;
        this.counter = 0;
        this.computed = new AtomicInteger(0);
        this.inflight = new AtomicInteger(0);
        this.assigned = new AtomicInteger(1);
        this.error = new AtomicInteger(0);
        this.sampler = new Sampler();
        this.concurrentLimitProcessor = new ConcurrentLimitProcessor(threads, sampler);
        this.sampler.registerObserver(concurrentLimitProcessor);
        this.predictor = new Predictor();
//        for (int i = 0, len = (int) (threads * 0.8); i < len; ++i) {
//            Supervisor.workLoads.add(new WorkLoad(port, 2 + ThreadLocalRandom.current().nextDouble()));
//        }
        //scheduledExecutorService = Executors.newScheduledThreadPool(threads / 3, new NamedInternalThreadFactory("concurrent-timer", true));
    }

    public double getPredict() {
        return averageRTT * Math.exp(concurrency / averageRTT);
    }

    public double getConcurrencyRatio() {
        return (concurrency + 0D) / threads;
    }

    public double getWeight() {
        return Math.pow(concurrentLimitProcessor.computingRateEstimated, 0.1) / Math.sqrt((getErrorRatio() + averageRTT / Supervisor.getMaxAvgRTT()) * getConcurrencyRatio());
        //return Math.sqrt((getErrorRatio() + averageRTT / Supervisor.getMaxAvgRTT()) * getConcurrencyRatio());
    }

    public long getLatencyThreshold() {
        //return (long) (Math.pow(5000, 1D / concurrency) > 1.4 ? Math.pow(1.4, concurrency) * Math.sqrt(concurrency) : 5000 * (1.4 - Math.pow(5000, 1D / concurrency)));
        //return (long) (Math.max(concurrentLimitProcessor.RTPropEstimated, 1) * ThreadLocalRandom.current().nextDouble(1, 2 + getConcurrencyRatio() - getErrorRatio()));
        //return (long) (Math.max(Math.sqrt(getPredict()) * ThreadLocalRandom.current().nextDouble(0.4, 0.6), 1));
        //return (long) (Math.max(Math.sqrt(getPredict()), 1));
        //return Math.max((long) (this.averageRTT * 1.1), 7);
        //return (long) Math.ceil(esRtt + varRtt * ThreadLocalRandom.current().nextDouble(2, 3 + getConcurrencyRatio() - getErrorRatio()));
        return Math.round(predictor.getPrediction() * ThreadLocalRandom.current().nextDouble(2, 3 + getConcurrencyRatio() - getErrorRatio()));
        //return Math.round(predictor.getPrediction() * (2D + getConcurrencyRatio() - getErrorRatio()));
        //return Math.round(ThreadLocalRandom.current().nextDouble(1, 2 + getConcurrencyRatio() - getErrorRatio()) * 10);
    }

    public boolean isConcurrentLimited() {
        //return concurrentLimitProcessor.rateLimiter.tryAcquire();
        //LOGGER.info("inflight: {} bound: {} computing rate: {} concurrency: {}", inflight.get(), concurrentLimitProcessor.getInflightBound(), concurrentLimitProcessor.computingRateEstimated, concurrency);
        // return inflight.get() > concurrentLimitProcessor.getInflightBound(concurrency);
        return inflight.get() > concurrentLimitProcessor.getInflightBound();
    }

    public double getErrorRatio() {
        //logger.info("assigned: {} error: {} ratio: {}", assigned.get(), error.get(), (double) error.get() / assigned.get() / 3);
        return (double) error.get() / assigned.get();
    }

    private double esRtt = 1;

    private double varRtt = 0.5;

    private volatile boolean init = false;

    public volatile double lastRTT = 1;

    public void onComputed(long latency, int lastComputed) {
        double RTT = latency / 1e6;
        lastRTT = RTT;
        double computingRate = (computed.get() - lastComputed) / RTT;
        if (!init) {
            this.sampler.startSample();
            init = true;
        } else {
            this.sampler.onComputed(computingRate, RTT);
        }
        //       this.concurrentLimitProcessor.onACK2(RTT, computingRate);
        this.predictor.update(RTT);
//        varRtt = 0.75 * varRtt + 0.25 * Math.abs(RTT - esRtt);
//        esRtt = 0.875 * esRtt + 0.125 * RTT;
//        LOGGER.info("es: {} act:{} predic: {}", esRtt, RTT, predictor.getPrediction());
//        if (RTT < 3) {
//            privilege.incrementAndGet();
//        }
//        if (RTT < 5) {
//            for (int i = 0; i < 3; ++i) {
//                Supervisor.workLoads.pollLast();
//                Supervisor.workLoads.add(new WorkLoad(port, ThreadLocalRandom.current().nextDouble()));
//            }
//            //concurrentLimitProcessor.switchFillUp();
//        }
//        Supervisor.workLoads.add(new WorkLoad(port, RTT));

        //LOGGER.info("{}port#?{}#?{}#?{}#?{}#?{}", port, RTT, rate, inflight.get(), concurrentLimitProcessor.getInflightBound(), waiting.get());
        //this.recordLatency(RTT);
        //LOGGER.info("avg: {} {}", averageRTT, getPredict());
    }

    public void refreshErrorSampling() {
        long now = System.currentTimeMillis();
        if (now - lastSamplingTime > 10) {
            if (getErrorRatio() > 0.8) {
                LOGGER.info("{} infer crash | error ratio: {}", port, getErrorRatio());
            }
            assigned.set(1);
            error.set(0);
            lastSamplingTime = now;
        }
    }

    private synchronized void recordLatency(double latency) {
        ++counter;
        if (counter == 10) {
            // recentMaxLatency = (long) latency;
            averageRTT = sum / 10D;
            sum = counter = 0;
        } else {
            sum += latency;
            //recentMaxLatency = Math.max(recentMaxLatency, (long) latency);
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
