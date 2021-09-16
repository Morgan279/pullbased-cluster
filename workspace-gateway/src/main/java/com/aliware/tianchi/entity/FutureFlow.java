package com.aliware.tianchi.entity;


import org.apache.dubbo.rpc.RpcException;

import java.util.concurrent.CompletableFuture;
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

    public long getRetentionTime(){
        return System.currentTimeMillis() - boxingTime;
    }

    public boolean isDone() {
        return future.isDone();
    }

    public void forceTimeout() {
        ((CompletableFuture<T>) future).completeExceptionally(new RpcException("force timeout"));
    }

    public int getPort() {
        return port;
    }
}
