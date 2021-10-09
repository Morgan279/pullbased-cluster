package com.aliware.tianchi.processor;

import com.aliware.tianchi.entity.FutureFlow;
import com.aliware.tianchi.entity.Supervisor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

public class TimeoutProcessor<T> implements Runnable {

    private final Queue<FutureFlow<T>> futureFlowQueue;

    //   private final ConcurrentSkipListSet<FutureFlow<T>> concurrentSkipListSet = new ConcurrentSkipListSet<>(FutureFlow::compare);

    public TimeoutProcessor() {
        this.futureFlowQueue = new ConcurrentLinkedQueue<>();
        new Thread(this).start();
    }

    public void addFuture(Future<T> future) {
        futureFlowQueue.offer(new FutureFlow<>(future));
        //concurrentSkipListSet.add(new FutureFlow<>(future, port));
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        FutureFlow<T> futureFlow;
        while (true) {
            if ((futureFlow = futureFlowQueue.poll()) != null && !futureFlow.isDone()) {
                if (futureFlow.getRetentionTime() > Supervisor.getLatencyThreshold()) {
                    futureFlow.forceTimeout();
                    //virtualProvider.inflight.decrementAndGet();
                } else {
                    futureFlowQueue.add(futureFlow);
                }
            }
        }
    }
}
