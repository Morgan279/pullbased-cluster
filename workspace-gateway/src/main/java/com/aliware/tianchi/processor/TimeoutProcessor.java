package com.aliware.tianchi.processor;

import com.aliware.tianchi.entity.FutureFlow;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.*;

public class TimeoutProcessor<T> implements Runnable {

    private final Queue<FutureFlow<T>> futureFlowQueue;

    public TimeoutProcessor() {
        this.futureFlowQueue = new ConcurrentLinkedQueue<>();
        new Thread(this).start();
//        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
//        scheduledExecutorService.scheduleAtFixedRate(this,50,1, TimeUnit.MILLISECONDS);
    }

    public void addFuture(Future<T> future){
        futureFlowQueue.offer(new FutureFlow<>(future));
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        FutureFlow<T> futureFlow;
        while (true){
            if((futureFlow = futureFlowQueue.poll()) != null && !futureFlow.isDone() && futureFlow.getRetentionTime() > 500){
                if(ThreadLocalRandom.current().nextDouble() < (double)futureFlow.getRetentionTime() / 5000){
                    futureFlow.forceTimeout();
                }
            }
        }
    }
}
