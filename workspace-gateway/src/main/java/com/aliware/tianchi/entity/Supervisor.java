package com.aliware.tianchi.entity;

import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Supervisor {

    private final static Logger logger = LoggerFactory.getLogger(Supervisor.class);

    public static volatile double maxWeight = 0;

    private static final Map<Integer, VirtualProvider> virtualProviderMap = new ConcurrentHashMap<>();

    private static final Set<VirtualProvider> availableVirtualProviders = new ConcurrentHashSet<>();

    public static boolean isOutOfService() {
        return availableVirtualProviders.isEmpty();
    }


    public static <T> void registerProvider(Invoker<T> invoker) {
        int port = invoker.getUrl().getPort();
        int threads = Integer.parseInt(invoker.getUrl().getParameter("threads", "200"));
        virtualProviderMap.putIfAbsent(port, new VirtualProvider(port, threads));
        availableVirtualProviders.add(virtualProviderMap.get(port));
        RoundRobinProcessor.register(port);
        logger.info("register provider, port: " + port + " thread:" + threads);
    }

    public static VirtualProvider getVirtualProvider(int port) {
        return virtualProviderMap.get(port);
    }

    public static boolean isProviderAvailable(VirtualProvider virtualProvider) {
        return availableVirtualProviders.contains(virtualProvider);
    }
}
