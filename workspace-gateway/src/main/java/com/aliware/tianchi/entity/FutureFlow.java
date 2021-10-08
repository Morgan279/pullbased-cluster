package com.aliware.tianchi.entity;


import java.util.concurrent.Future;

public class FutureFlow<T> {

    private final int port;

    private final Future<T> future;

    private final long boxingTime;

    public FutureFlow(Future<T> future, int port) {
        this.future = future;
        this.port = port;
        this.boxingTime = System.currentTimeMillis();
    }

    public static <T> int compare(FutureFlow<T> f1, FutureFlow<T> f2) {
        return Long.compare(f2.getRetentionTime(), f1.getRetentionTime());
    }

    public long getRetentionTime() {
        return System.currentTimeMillis() - boxingTime;
    }

    public boolean isDone() {
        return future.isDone();
    }

    public void forceTimeout() {
        //((CompletableFuture<T>) future).completeExceptionally(new RpcException("force timeout"));
        future.cancel(true);
    }

    public int getPort() {
        return port;
    }
}
