package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.ProviderStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private static final int IMPERIUM_BOUND = 5;

    private static final int MAX_RT = 5000;

    private final int port;

    public ProviderStatus status;

    private final Stack<Long> timeoutStamp;

    private final AtomicInteger imperium;

    private final Map<Integer, AtomicInteger> latencyRecord;

    public VirtualProvider(int port) {
        this.port = port;
        this.timeoutStamp = new Stack<>();
        this.imperium = new AtomicInteger();
        this.latencyRecord = new HashMap<>(MAX_RT + 1);
        for (int i = 0; i <= MAX_RT; ++i) {
            latencyRecord.put(i, new AtomicInteger());
        }
    }

    public boolean hasImperium() {
        return imperium.get() > 0;
    }

    public void executeImperium() {
        imperium.decrementAndGet();
    }

    public double getWeight() {
        return 0;
    }

    public void recordLatency(long latency) {
        for (long i = IMPERIUM_BOUND - latency; i >= 0; --i) {
            imperium.incrementAndGet();
        }
    }

    public int getPort() {
        return this.port;
    }
}
