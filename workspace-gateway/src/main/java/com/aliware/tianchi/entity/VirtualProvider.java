package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.ProviderStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualProvider {

    private static final int IMPERIUM_BOUND = 5;

    private static final int MAX_RT = 5000;

    private final int port;

    private volatile ProviderStatus status;

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
