package com.aliware.tianchi.entity;


import java.util.concurrent.Future;

public class FutureFlow<T> {

    private final String id;

    private final int port;

    private final Future<T> future;

    private final long boxingTime;

    public FutureFlow(Future<T> future, int port, String id) {
        this.future = future;
        this.port = port;
        this.id = id;
        this.boxingTime = System.currentTimeMillis();
    }

    public long getRetentionTime(){
        return System.currentTimeMillis() - boxingTime;
    }

    public boolean isDone() {
        return future.isDone();
    }

    public void forceTimeout() {
        future.cancel(true);
    }

    public String getId() {
        return id;
    }

    public int getPort() {
        return port;
    }
}
