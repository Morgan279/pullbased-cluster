package com.aliware.tianchi.entity;

public class WorkLoad {
    public int port;

    public double latency;

    public WorkLoad(int port, double latency) {
        this.port = port;
        this.latency = latency;
    }

    public static int compare(WorkLoad w1, WorkLoad w2) {
        return Double.compare(w1.latency, w2.latency);
    }
}
