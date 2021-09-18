package com.aliware.tianchi.processor;

import com.aliware.tianchi.entity.FutureFlow;
import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

public class TimeoutProcessor<T> implements Runnable {

    private final Queue<FutureFlow<T>> futureFlowQueue;

    public TimeoutProcessor() {
        this.futureFlowQueue = new ConcurrentLinkedQueue<>();
        new Thread(this).start();
    }

    public void addFuture(Future<T> future, int port) {
        futureFlowQueue.offer(new FutureFlow<>(future, port));
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        FutureFlow<T> futureFlow;
        while (true) {
            if ((futureFlow = futureFlowQueue.poll()) != null && !futureFlow.isDone()) {
                VirtualProvider virtualProvider = Supervisor.getVirtualProvider(futureFlow.getPort());
                if (futureFlow.getRetentionTime() > virtualProvider.getLatencyThreshold()) {
                    futureFlow.forceTimeout();
                } else {
                    futureFlowQueue.add(futureFlow);
                }
            }
        }
    }
}
