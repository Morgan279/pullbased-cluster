package com.aliware.tianchi.entity;

import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.apache.dubbo.rpc.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Supervisor {

    private final static Logger LOGGER = LoggerFactory.getLogger(Supervisor.class);

    private static final Map<Integer, VirtualProvider> virtualProviderMap = new ConcurrentHashMap<>();

    public static <T> void registerProvider(Invoker<T> invoker) {
        int port = invoker.getUrl().getPort();
        int threads = Integer.parseInt(invoker.getUrl().getParameter("threads", "200"));
        virtualProviderMap.putIfAbsent(port, new VirtualProvider(port, threads));
        RoundRobinProcessor.register(port);
        LOGGER.info("register provider, port: " + port + " thread:" + threads);
    }

    public static VirtualProvider getVirtualProvider(int port) {
        return virtualProviderMap.get(port);
    }

}
