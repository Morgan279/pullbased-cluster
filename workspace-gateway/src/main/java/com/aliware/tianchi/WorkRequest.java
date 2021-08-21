package com.aliware.tianchi;

import java.util.concurrent.ThreadLocalRandom;

public class WorkRequest {
    final private int port;
    final private Double latency;

    public WorkRequest(int port, long latency) {
        this.port = port;
        this.latency = latency + ThreadLocalRandom.current().nextDouble();
    }

    public Double getLatency(){
        return this.latency;
    }

    public int getPort() {
        return port;
    }
}
