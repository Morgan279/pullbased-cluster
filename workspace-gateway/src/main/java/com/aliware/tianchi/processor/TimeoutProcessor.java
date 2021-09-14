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
//        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
//        scheduledExecutorService.scheduleAtFixedRate(this, 50, 1, TimeUnit.MILLISECONDS);
    }

    public void addFuture(Future<T> future, int port, long id) {
        futureFlowQueue.offer(new FutureFlow<>(future, port, id));
    }

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        FutureFlow<T> futureFlow;
        while (true) {
            if ((futureFlow = futureFlowQueue.poll()) != null && !futureFlow.isDone()) {
                VirtualProvider virtualProvider = Supervisor.getVirtualProvider(futureFlow.getPort());
//                long retentionTime = futureFlow.getRetentionTime();
//                double inference = virtualProvider.getTimeoutInferenceProbability(retentionTime) * 0.74 / virtualProvider.getCdf(retentionTime);
//                if (!futureFlow.isDone() && ThreadLocalRandom.current().nextDouble() < inference) {
//                    virtualProvider.addInference(futureFlow.getId(), retentionTime);
//                    futureFlow.forceTimeout();
//                }
                if (futureFlow.getRetentionTime() > virtualProvider.getP999Latency()) {
                    futureFlow.forceTimeout();
                } else futureFlowQueue.add(futureFlow);
            }
        }
    }
}
