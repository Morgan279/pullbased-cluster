package com.aliware.tianchi.tool;

public class StopWatch {

    private long lastStartNanoSec = 0;

    public synchronized void start() {
        lastStartNanoSec = System.nanoTime();
    }

    public synchronized double stop() {
        return (System.nanoTime() - lastStartNanoSec) / 1e6;
    }

}
