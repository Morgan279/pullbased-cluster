package com.aliware.tianchi.entity;

public class WorkLoad {
    public int port;

    public long latency;

    public WorkLoad(int port, long latency) {
        this.port = port;
        this.latency = latency;
    }

    public static int compare(WorkLoad w1, WorkLoad w2) {
        return Long.compare(w1.latency, w2.latency);
    }
}
