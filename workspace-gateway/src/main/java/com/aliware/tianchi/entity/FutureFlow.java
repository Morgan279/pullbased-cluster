package com.aliware.tianchi.entity;


import java.util.concurrent.Future;

public class FutureFlow<T> {

    private final Future<T> future;

    private final long boxingTime;


    public FutureFlow(Future<T> future) {
        this.future = future;
        this.boxingTime = System.currentTimeMillis();
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
}
