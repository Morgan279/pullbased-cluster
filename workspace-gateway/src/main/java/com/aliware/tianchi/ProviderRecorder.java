package com.aliware.tianchi;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ProviderRecorder {

    static ConcurrentHashMap<Integer, ProviderRecorder> providerMap = new ConcurrentHashMap<>();

    static ConcurrentSkipListSet<WorkRequest> requestQueue = new ConcurrentSkipListSet<>(Comparator.comparingDouble(WorkRequest::getLatency));

    static void recordLatency(int port, long latency) {
        //providerMap.get(port).recordLatency(latency);
        if (latency != -1) {
            if (latency < 11) {
                requestQueue.pollLast();
                requestQueue.add(new WorkRequest(port, latency));
            }
            requestQueue.add(new WorkRequest(port, 50));
        }
    }

    static WorkRequest select() {
        return requestQueue.pollFirst();
    }

    final int port;
    final int maxThreads;

    void request(long latency) {
        requestQueue.add(new WorkRequest(port, latency));
    }

    public ProviderRecorder(int port, int reportedMax) {
        this.port = port;
        this.maxThreads = reportedMax;
    }

    synchronized void initialRequests() {
        for (int i = 0; i < maxThreads; i++) {
            request(50);
        }
        //System.out.println("complete initialization for port: " + port + ", maxthreads: " + maxThreads);
    }


    private void recordLatency(long latency) {
        if (latency != -1) {
            if (latency < 11) {
                requestQueue.pollLast();
                requestQueue.add(new WorkRequest(port, latency));
            }
            request(50);
        }
    }
}
