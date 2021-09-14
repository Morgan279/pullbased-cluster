package com.aliware.tianchi.entity;

import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Supervisor {

    private final static Logger logger = LoggerFactory.getLogger(Supervisor.class);


    public static volatile double maxWeight = 0;

    private static final int CRASH_RESTART_INTERVAL = 1;

    private static final int CURRENT_RESTART_INTERVAL = 1;

    private static final Map<Integer, VirtualProvider> virtualProviderMap = new ConcurrentHashMap<>();

    private static final Set<VirtualProvider> availableVirtualProviders = new ConcurrentHashSet<>();

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3, new NamedInternalThreadFactory("crash-restart-timer", true));

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

    public static void notifyCrash(int port) {
        VirtualProvider provider = virtualProviderMap.get(port);
        availableVirtualProviders.remove(provider);
        scheduleRestart(provider, 100);
    }

    public synchronized static void executeCurrentLimit(int port) {
        VirtualProvider provider = virtualProviderMap.get(port);
        if (!isProviderAvailable(provider)) return;

        availableVirtualProviders.remove(provider);
        scheduleRestart(provider, CURRENT_RESTART_INTERVAL);
    }

    private static void scheduleRestart(VirtualProvider provider, int restartIntervalSecond) {
        scheduledExecutorService.schedule(() -> {
            provider.restart();
            availableVirtualProviders.add(provider);
        }, restartIntervalSecond, TimeUnit.MILLISECONDS);
    }
}
