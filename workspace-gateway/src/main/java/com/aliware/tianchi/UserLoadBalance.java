package com.aliware.tianchi;

import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.apache.dubbo.common.URL;
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

    private double globalMaxWeight = 0;

    private Map<Integer, AtomicInteger> map = new HashMap<>();

    private final int[] concurrentWeightArray = {3, 2, 1};

    private final int[] RTWeightArray = {4, 3, 2};

    private final int[] ErrorWeightArray = {4, 3, 2};

    private final int[] P999WeightArray = {5, 4, 3};

    private final int[] ThreadWeightArray = {4, 3, 2};

    private final int[] IWeightArray = {9, 4, 3};

    private final int[] RandomWeightArray = {3, 2, 1};

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        return MaxWeightDispatchProcessor.select(invokers);

        for (Invoker<T> invoker : invokers) {
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            if (virtualProvider.hasImperium()) {
                virtualProvider.executeImperium();
                return invoker;
            }
        }

        Map<Integer, Integer> weightMap = new HashMap<>(invokers.size());
        List<VirtualProvider> virtualProviderList = new ArrayList<>(invokers.size());
        for (Invoker<T> invoker : invokers) {
            //map.putIfAbsent(invoker.getUrl().getPort(), new AtomicInteger());
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            weightMap.put(virtualProvider.getPort(), 0);
            virtualProviderList.add(virtualProvider);
            //System.out.println(virtualProvider.getPort() + "'s weight: " + virtualProvider.getWeight(maxWeight) + " num: " + map.get(virtualProvider.getPort()).get());
        }

        accumulateWeight(weightMap, virtualProviderList, RTWeightArray, Comparator.comparingDouble(VirtualProvider::getRTWeight));
        accumulateWeight(weightMap, virtualProviderList, concurrentWeightArray, Comparator.comparingInt(VirtualProvider::getConcurrent));
        accumulateWeight(weightMap, virtualProviderList, ErrorWeightArray, Comparator.comparingInt(VirtualProvider::getRecentErrorSize));
        accumulateWeight(weightMap, virtualProviderList, P999WeightArray, Comparator.comparingLong(VirtualProvider::getP999Latency));

        int selectedPort = RoundRobinProcessor.select(weightMap);
        for (Invoker<T> invoker : invokers) {
            if (invoker.getUrl().getPort() == selectedPort) return invoker;
        }

        throw new RpcException("there is no available provider");
    }

    private void accumulateWeight(Map<Integer, Integer> weightMap, List<VirtualProvider> virtualProviderList, int[] weightArray, Comparator<VirtualProvider> virtualProviderComparator) {
        int len = Math.min(virtualProviderList.size(), weightArray.length);
        virtualProviderList.sort(virtualProviderComparator);
        for (int i = 0; i < len; ++i) {
            int port = virtualProviderList.get(i).getPort();
            weightMap.put(port, weightMap.get(port) + weightArray[i]);
        }
    }

}
