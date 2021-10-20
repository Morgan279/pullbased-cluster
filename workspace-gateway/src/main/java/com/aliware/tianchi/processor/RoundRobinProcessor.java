package com.aliware.tianchi.processor;

import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import io.netty.util.internal.ThreadLocalRandom;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoundRobinProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(RoundRobinProcessor.class);

    private static final Map<Integer, Integer> currentWeightMap = new HashMap<>();

    public static void register(int port) {
        currentWeightMap.putIfAbsent(port, 0);
    }

    public static void reset() {
        synchronized (currentWeightMap) {
            currentWeightMap.replaceAll((p, v) -> 0);
        }
    }


    public static int select(Map<Integer, Integer> weightMap) {
        int totalWeight = 0;
        int maxWeight = -1;
        int maxWeightPort = -1;

        synchronized (currentWeightMap) {
            for (Integer port : weightMap.keySet()) {
                int weight = weightMap.get(port);
                totalWeight += weight;
                int currentWeight = currentWeightMap.get(port) + weight;
                currentWeightMap.put(port, currentWeight);
                if (currentWeight > maxWeight) {
                    maxWeight = currentWeight;
                    maxWeightPort = port;
                }
            }
            currentWeightMap.put(maxWeightPort, maxWeight - totalWeight);
        }

        return maxWeightPort;
    }

    public static int selectMaxWeight(Map<Integer, Integer> weightMap) {
        int maxWeight = -1;
        int maxWeightPort = -1;

        for (Integer port : weightMap.keySet()) {
            int weight = weightMap.get(port);
            if (weight > maxWeight) {
                maxWeight = weight;
                maxWeightPort = port;
            }
        }

        return maxWeightPort;
    }

    public static <T> Invoker<T> selectMaxWeight(List<Invoker<T>> invokers) {
        int sumWeight = 0;
        int lastWeight = 0;
        boolean sameWeight = true;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0, len = invokers.size(); i < len; ++i) {
            Invoker<T> invoker = invokers.get(i);
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            virtualProvider.weight = (int) Math.round(virtualProvider.concurrentLimitProcessor.computingRateEstimated);
            sumWeight += virtualProvider.weight;
            stringBuilder.append(virtualProvider.weight).append("|").append(virtualProvider.concurrentLimitProcessor.getInflightBound()).append("|").append(virtualProvider.concurrentLimitProcessor.computingRateEstimated).append(" ");
            if (i > 0 && sameWeight && virtualProvider.weight != lastWeight) {
                sameWeight = false;
            }
            lastWeight = virtualProvider.weight;
        }
        LOGGER.info("weights: {}", stringBuilder.toString());

        if (!sameWeight) {
            int offset = ThreadLocalRandom.current().nextInt(sumWeight);
            for (Invoker<T> invoker : invokers) {
                VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
                offset -= virtualProvider.weight;
                if (offset < 0) {
                    return invoker;
                }
            }
        }
        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }

    public static <T> Invoker<T> selectMinRTTInvoker(List<Invoker<T>> invokers) {
        Invoker<T> minInv = null;
        double minRTT = Double.MAX_VALUE;
        for (Invoker<T> invoker : invokers) {
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            if (virtualProvider.lastRTT < minRTT) {
                minInv = invoker;
                minRTT = virtualProvider.lastRTT;
            }
        }

        return minInv;
    }

    public static <T> Invoker<T> selectMinWaitingInvoker(List<Invoker<T>> invokers) {
        int sumWeight = 0;
        boolean sameWeight = true;
        int lastWeight = 0;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0, len = invokers.size(); i < len; ++i) {
            Invoker<T> invoker = invokers.get(i);
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            sumWeight += virtualProvider.waiting;
            stringBuilder.append(virtualProvider.waiting).append("|").append(virtualProvider.concurrentLimitProcessor.getInflightBound()).append("|").append(virtualProvider.concurrentLimitProcessor.computingRateEstimated).append(" ");
            if (i > 0 && sameWeight && virtualProvider.waiting != lastWeight) {
                sameWeight = false;
            }
            lastWeight = virtualProvider.waiting;
        }
//        LOGGER.info("waiting: {}", stringBuilder.toString());
        if (sumWeight > 3 && !sameWeight) {
            int offset = ThreadLocalRandom.current().nextInt(sumWeight << 1);
            for (Invoker<T> invoker : invokers) {
                VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
                offset -= sumWeight - virtualProvider.waiting;
                if (offset < 0) {
                    return invoker;
                }
            }
        }

        return selectMinRTTInvoker(invokers);
        //return selectMaxWeight(invokers);
        //return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
    }
}
