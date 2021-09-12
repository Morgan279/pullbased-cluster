package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.constant.ProviderStatus;
import com.aliware.tianchi.processor.RoundRobinProcessor;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private static final int IMPERIUM_BOUND = 3;

    private static final int MAX_RT = 5000;

    public final AtomicInteger currentLimiter;

    public int threads;

    private final int N = Config.SAMPLING_COUNT;

    private final int port;

    private volatile ProviderStatus status;

    private volatile double threadFactor;

    private volatile int concurrent;

    private final Stack<Long> timeoutStamp;

    private final AtomicInteger imperium;

    private final List<Long> timeoutRequests;

    private int counter;

    private long sum;

    private double initialLambda;

    private double currentLambda;

    public Map<Long, Long> inferenceRecords = new HashMap<>();

    public VirtualProvider(int port, int threads) {
        this.port = port;
        this.threads = threads;
        this.threadFactor = threads / 10d;
        this.currentLimiter = new AtomicInteger((int) (threads * 14.4));
        this.timeoutStamp = new Stack<>();
        this.imperium = new AtomicInteger();
        this.timeoutRequests = new ArrayList<>();
        this.init();
    }

    private void init() {
        this.sum = 0;
        this.counter = 0;
        this.initialLambda = 0;
        this.currentLambda = 0.015;
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

    public void restart() {
        if (ProviderStatus.AVAILABLE.equals(this.status)) return;

        this.imperium.set(0);
        this.timeoutStamp.clear();
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

    public double getWeight(double maxWeight) {
        double lambdaDiff = currentLambda - initialLambda;
        double RTWeight = lambdaDiff > 0 ? lambdaDiff * 10 : 1;
        System.out.println(port + "'s RT weight : " + RTWeight);
        //System.out.println("RTWeight: " + RTWeight + " thread factor: " + threadFactor + " weight: " + RTWeight * threadFactor);
        double weight = RTWeight + ThreadLocalRandom.current().nextDouble(threads / 2000d + Math.abs(maxWeight - RTWeight)) + (double) threads / (concurrent + 1) / 300;
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
                sum = 0;
                counter = 0;
            }
        }
    }

    public synchronized void recordTimeout() {
        long now = System.currentTimeMillis();
        while (!timeoutStamp.isEmpty() && now - timeoutStamp.peek() > 5) {
            timeoutStamp.pop();
        }
        timeoutStamp.push(now);
        if (timeoutStamp.size() >= 30) {
            this.crush();
        }
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

    public int getPort() {
        return this.port;
    }

    public void setThreadFactor(double threadFactor) {
        this.threadFactor = threadFactor;
    }

    public void setConcurrent(int concurrent) {
        if (concurrent == 0) {
            imperium.incrementAndGet();
            RoundRobinProcessor.reset();
        }
        this.concurrent = concurrent;
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
