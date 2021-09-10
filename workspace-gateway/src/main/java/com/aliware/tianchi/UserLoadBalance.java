package com.aliware.tianchi;

import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    private double globalMaxWeight = 0;

    private Map<Integer, AtomicInteger> map = new HashMap<>();

    private final int[] concurrentWeightArray = {7, 4, 3};

    private final int[] RTWeightArray = {3, 2, 1};

    private final int[] IWeightArray = {4, 1, 1};

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        double maxWeight = -1;

        Map<Integer, Integer> weightMap = new HashMap<>(invokers.size());
        List<VirtualProvider> virtualProviderList = new ArrayList<>(invokers.size());
        for (Invoker<T> invoker : invokers) {
            map.putIfAbsent(invoker.getUrl().getPort(), new AtomicInteger());
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            weightMap.put(virtualProvider.getPort(), 0);
            virtualProviderList.add(virtualProvider);
            LOGGER.info(virtualProvider.getPort() + "'s weight: " + virtualProvider.getWeight(maxWeight) + " num: " + map.get(virtualProvider.getPort()).get());
            //System.out.println(virtualProvider.getPort() + "'s weight: " + virtualProvider.getWeight(maxWeight) + " num: " + map.get(virtualProvider.getPort()).get());
        }

        virtualProviderList.sort(Comparator.comparingDouble(VirtualProvider::getRTWeight).reversed());
        for (int i = 0; i < RTWeightArray.length; ++i) {
            int port = virtualProviderList.get(i).getPort();
            weightMap.put(port, weightMap.get(port) + RTWeightArray[i]);
        }

        virtualProviderList.sort(Comparator.comparingInt(VirtualProvider::getImperium).reversed());
        for (int i = 0; i < RTWeightArray.length; ++i) {
            int port = virtualProviderList.get(i).getPort();
            weightMap.put(port, weightMap.get(port) + IWeightArray[i]);
        }

        int totalWeight = 0;
        VirtualProvider maxWeightProvider = null;
        virtualProviderList.sort(Comparator.comparingDouble(VirtualProvider::getFactor).reversed());
        for (int i = 0; i < concurrentWeightArray.length; ++i) {
            VirtualProvider virtualProvider = virtualProviderList.get(i);
            int port = virtualProvider.getPort();
            int weight = weightMap.get(port) + concurrentWeightArray[i];
            totalWeight += weight;
            virtualProviderList.get(i).currentWeight += weight;
            if (virtualProviderList.get(i).currentWeight > maxWeight) {
                maxWeight = virtualProviderList.get(i).currentWeight;
                maxWeightProvider = virtualProvider;
            }
        }

        maxWeightProvider.currentWeight -= totalWeight;

        for (Invoker<T> invoker : invokers) {
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            if (Supervisor.isProviderAvailable(virtualProvider)) {
                LOGGER.info(invoker.getUrl().getPort() + ": weight: " + virtualProvider.getWeight(globalMaxWeight));
                if (virtualProvider.getPort() == maxWeightProvider.getPort()) {
                    map.get(virtualProvider.getPort()).incrementAndGet();
                    if (virtualProvider.hasImperium()) virtualProvider.executeImperium();
                    return invoker;
                }
            }
        }

        throw new RpcException("there is no available provider");

//        Invoker<T> selectedInvoker = null;
//        for (Invoker<T> invoker : invokers) {
//            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
//            if (Supervisor.isProviderAvailable(virtualProvider)) {
//                LOGGER.info(invoker.getUrl().getPort() + ": weight: " + virtualProvider.getWeight(globalMaxWeight));
//                if (virtualProvider.hasImperium()) {
//                    virtualProvider.executeImperium();
//                    map.get(virtualProvider.getPort()).incrementAndGet();
//                    return invoker;
//                } else if (virtualProvider.getWeight(globalMaxWeight) > maxWeight) {
//                    selectedInvoker = invoker;
//                    maxWeight = virtualProvider.getWeight(globalMaxWeight);
//                }
//            }
//        }
//
//        if (selectedInvoker == null) {
//            throw new RpcException("there is no available provider");
//        }
//
//        globalMaxWeight = maxWeight;
//        map.get(selectedInvoker.getUrl().getPort()).incrementAndGet();
//        return selectedInvoker;
    }

}
