package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.Config;
import com.aliware.tianchi.constant.ProviderStatus;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private static final int IMPERIUM_BOUND = 5;

    private static final int MAX_RT = 5000;

    private final int N = Config.SAMPLING_COUNT;

    private final int port;

    private volatile ProviderStatus status;

    private final Stack<Long> timeoutStamp;

    private final AtomicInteger imperium;

    private final List<Long> timeoutRequests;

    private int counter;

    private long sum;

    private double initialLambda;

    private double currentLambda;

    public Map<String, Long> inferenceRecords = new HashMap<>();

    public VirtualProvider(int port) {
        this.port = port;
        this.timeoutStamp = new Stack<>();
        this.imperium = new AtomicInteger();
        this.timeoutRequests = new ArrayList<>();
        this.sum = 0;
        this.counter = 0;
        this.initialLambda = 0;
        this.currentLambda = 0.015;
    }

    public void addInference(String id, long retentionTime) {
        inferenceRecords.put(id, retentionTime);
    }

    public void restart() {
        if (ProviderStatus.AVAILABLE.equals(this.status)) return;

        this.imperium.set(0);
        this.timeoutStamp.clear();
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

    private Set<String> correctId = new HashSet<>();

    public synchronized void recordTimeoutRequestId(String id) {
        if (inferenceRecords.containsKey(id)) correctId.add(id);
        timeoutRequests.add(Optional.ofNullable(inferenceRecords.get(id)).orElse(4800L));
//        System.out.println("time out num: " + timeoutRequests.size() + " correct num: " + correctId.size() + " correct radio: " + ((double) correctId.size() / inferenceRecords.keySet().size()));
//        int errorT = 0;
//        int correctT = 0;
//        for (String inferenceId : inferenceRecords.keySet()) {
//            long inferenceTime = inferenceRecords.get(inferenceId);
//            if (!correctId.contains(inferenceId))
//                errorT += inferenceTime;
//            else
//                correctT += 5000 - inferenceTime;
//        }
//        System.out.println("save time: " + (correctT - errorT));
    }

    public double getTimeoutInferenceProbability(long retentionTime) {
        if (timeoutRequests.isEmpty()) return 0;
        int num = 0;
        for (Long time : timeoutRequests) {
            if (time >= retentionTime) ++num;
        }
        return (double) num / timeoutRequests.size();
    }

    public double getWeight() {
        return 0;
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

    public double getCdf(long retentionTime) {
        return Math.exp(-currentLambda * retentionTime);
    }

    private void refreshLambda() {
        double value = (double) N / sum;
        if (initialLambda == 0) {
            initialLambda = value;
        }
        currentLambda = value;
        System.out.println("currentLambda: " + currentLambda + " initial: " + initialLambda);
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
