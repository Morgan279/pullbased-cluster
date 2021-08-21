package com.aliware.tianchi.entity;

import com.aliware.tianchi.constant.ProviderInfo;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Supervisor {

    private static final ConcurrentHashMap<Integer,VirtualProvider> virtualProviderMap = new ConcurrentHashMap<>();

    public static boolean hasAvailableProvider(){
        return true;
    }

    public static void registerProvider(int port){
        virtualProviderMap.putIfAbsent(port,new VirtualProvider(port));
    }

    public static VirtualProvider getVirtualProvider(int port){
        return virtualProviderMap.get(port);
    }

    public static void recordLatency(int port,long latency){
        virtualProviderMap.get(port).recordLatency(latency);
    }

    public static Collection<VirtualProvider> getAvailableProviders(){
        return virtualProviderMap.values();
    }
}
