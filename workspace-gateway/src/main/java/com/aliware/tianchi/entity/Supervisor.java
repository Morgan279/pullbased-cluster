package com.aliware.tianchi.entity;

import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class Supervisor {

    private final static Logger LOGGER = LoggerFactory.getLogger(Supervisor.class);

    public static ConcurrentSkipListSet<WorkLoad> workLoads = new ConcurrentSkipListSet<>(WorkLoad::compare);

    public static final Map<Integer, VirtualProvider> virtualProviderMap = new ConcurrentHashMap<>();

    //public static Invoker lastReturned;

    public static <T> void registerProvider(Invoker<T> invoker) {
        int port = invoker.getUrl().getPort();
        int threads = Integer.parseInt(invoker.getUrl().getParameter(CommonConstants.THREADS_KEY, String.valueOf(CommonConstants.DEFAULT_THREADS)));
        virtualProviderMap.putIfAbsent(port, new VirtualProvider(port, threads));
        RoundRobinProcessor.register(port);
        LOGGER.info("register provider, port: " + port + " thread:" + threads);
        //lastReturned = invoker;
    }

    public static VirtualProvider getVirtualProvider(int port) {
        return virtualProviderMap.get(port);
    }

    public static long getLatencyThreshold() {
        long threshold = 5000;
        for (VirtualProvider virtualProvider : virtualProviderMap.values()) {
            threshold = Math.min(threshold, virtualProvider.recentMaxLatency);
        }
        return Math.max(threshold, 20);
    }

    public static double getMaxAvgRTT() {
        double maxAvg = 0;
        for (VirtualProvider virtualProvider : virtualProviderMap.values()) {
            maxAvg = Math.max(maxAvg, virtualProvider.averageRTT);
        }
        return maxAvg;
    }
}
