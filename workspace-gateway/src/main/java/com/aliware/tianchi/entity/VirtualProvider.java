package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.constant.ProviderStatus;
import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private final static Logger logger = LoggerFactory.getLogger(VirtualProvider.class);

    public long lastInvokeTime = System.currentTimeMillis();

    private static final int IMPERIUM_BOUND = 5;

    private static final int MAX_RT = 5000;

    public int threads;

    private final int N = Config.SAMPLING_COUNT;

    private final int port;

    public volatile ProviderStatus status;

    private volatile double threadFactor;

    private volatile int concurrent;

    private volatile int remainThreadCount;

    private final Deque<Long> errorStamp;

    private final AtomicInteger imperium;

    private final List<Long> timeoutRequests;

    private int counter;

    private long sum;

    private double initialLambda;

    private double currentLambda;

    private double permitThreadsFactor;

    public AtomicInteger inflight = new AtomicInteger();

    private final double SAMPLE_FACTOR = 0.99;

    private final int queueLength = (int) (Config.SAMPLING_COUNT * (1 - SAMPLE_FACTOR));

    private final PriorityQueue<Long> p99Latency = new PriorityQueue<>(queueLength, Long::compare);

    public Map<Long, Long> inferenceRecords = new HashMap<>();

    public VirtualProvider(int port, int threads) {
        this.port = port;
        this.threads = threads;
        this.remainThreadCount = threads;
        this.concurrent =threads + (int)Math.sqrt(threads);
        this.threadFactor = threads / 10d;
        this.errorStamp = new ArrayDeque<>();
        this.imperium = new AtomicInteger();
        this.timeoutRequests = new ArrayList<>();
        this.init();
    }

    private void init() {
        this.sum = 0;
        this.counter = 0;
        this.initialLambda = 0;
        this.currentLambda = 0.015;
        this.permitThreadsFactor = 0.8;
        this.status = ProviderStatus.AVAILABLE;
    }

    public double getThreadFactor(){
        return this.threadFactor;
    }

    public double getRandomWeight(){
        return ThreadLocalRandom.current().nextDouble(1);
    }

    public double getFactor() {
        return (double) (threads - concurrent) / threads;
    }

    public int getConcurrent() {
        return this.concurrent;
    }

    public void addInference(long id, long retentionTime) {
        inferenceRecords.put(id, retentionTime);
    }

    public volatile long averageRT = 10;

    public long getLatencyThreshold() {
        return Math.max((long) (this.averageRT * 1.5), 10);
    }

    public boolean tryRequireConcurrent() {
        //logger.info("remain: {}", remainThreadCount);
        return remainThreadCount > 0;
        //return true;
    }


    public void restart() {
        if (ProviderStatus.AVAILABLE.equals(this.status)) return;

        this.imperium.set(0);
        this.errorStamp.clear();
        this.init();
        this.status = ProviderStatus.AVAILABLE;
    }

    public boolean hasImperium() {
        return imperium.get() > 0;
    }

    public void currentLimit() {
        this.status = ProviderStatus.UNAVAILABLE;
    }

    private void crush() {
        this.status = ProviderStatus.UNAVAILABLE;
        Supervisor.notifyCrash(this.port);
    }

    public void executeImperium() {
        imperium.decrementAndGet();
    }

    private Set<Long> correctId = new HashSet<>();

    public synchronized void recordTimeoutRequestId(long id) {
        if (inferenceRecords.containsKey(id)) correctId.add(id);
        timeoutRequests.add(Optional.ofNullable(inferenceRecords.get(id)).orElse(5000L));
        //recordTimeout();
    }

    private void printInferenceProbability() {
        System.out.println("time out num: " + timeoutRequests.size() + " correct num: " + correctId.size() + " correct radio: " + ((double) correctId.size() / inferenceRecords.keySet().size()));
        int errorT = 0;
        int correctT = 0;
        for (Long inferenceId : inferenceRecords.keySet()) {
            long inferenceTime = inferenceRecords.get(inferenceId);
            if (!correctId.contains(inferenceId))
                errorT += inferenceTime;
            else
                correctT += 5000 - inferenceTime;
        }
        System.out.println("save time: " + (correctT - errorT));
    }

    public double getTimeoutInferenceProbability(long retentionTime) {
        if (timeoutRequests.isEmpty()) return 0;
        int num = 0;
        for (Long time : timeoutRequests) {
            if (time >= retentionTime) ++num;
        }
        return (double) num / timeoutRequests.size();
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
        for (long i = IMPERIUM_BOUND - latency; i >= 0; --i) {
            imperium.incrementAndGet();
        }
        synchronized (this) {
            sum += latency;
            ++counter;
            if (counter == N) {
                refreshLambda();
                averageRT = sum / counter;
                sum = 0;
                counter = 0;
            }
            if (p99Latency.size() < queueLength) {
                p99Latency.add(latency);
            } else if (latency > p99Latency.peek()) {
                p99Latency.poll();
                p99Latency.add(latency);
            }
        }
    }

    public long getP999Latency() {
        return Optional.ofNullable(p99Latency.peek()).orElse(100L);
    }

    public synchronized void recordError() {
        long now = System.currentTimeMillis();
        while (!errorStamp.isEmpty() && (now - errorStamp.getFirst() > 5)) {
            errorStamp.removeFirst();
        }
        errorStamp.addLast(now);
    }

    public int getRecentErrorSize() {
        return errorStamp.size();
    }

    public double getCdf(long retentionTime) {
        return Math.exp(-currentLambda * retentionTime);
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
