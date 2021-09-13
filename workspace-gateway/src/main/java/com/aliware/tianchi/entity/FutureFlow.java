package com.aliware.tianchi.entity;


import org.apache.dubbo.rpc.RpcException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class FutureFlow<T> {

    private final long id;

    private final int port;

    private final Future<T> future;

    private final long boxingTime;

    public FutureFlow(Future<T> future, int port, long id) {
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
        ((CompletableFuture<T>) future).completeExceptionally(new RpcException("force timeout"));
    }

    public long getId() {
        return id;
    }

    public int getPort() {
        return port;
    }
}
