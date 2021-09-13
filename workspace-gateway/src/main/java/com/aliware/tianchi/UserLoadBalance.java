package com.aliware.tianchi;

import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final int[] ThreadWeightArray = {4, 3, 2};

    private final int[] IWeightArray = {9, 4, 3};

    private final int[] RandomWeightArray = {3, 2, 1};

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        double maxWeight = -1;
        Invoker<T> selectedInvoker = null;
        for (Invoker<T> invoker : invokers) {
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            if (Supervisor.isProviderAvailable(virtualProvider)) {
                //LOGGER.info(invoker.getUrl().getPort() + ": weight: " + virtualProvider.getWeight());
                if (virtualProvider.hasImperium()) {
                    virtualProvider.executeImperium();
                    return invoker;
                } else if (virtualProvider.getWeight() > maxWeight) {
                    selectedInvoker = invoker;
                    maxWeight = virtualProvider.getWeight();
                }
            }
        }

        if (selectedInvoker == null) {
            throw new RpcException("there is no available provider");
        }

        return selectedInvoker;

//        for (Invoker<T> invoker : invokers) {
//            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
//            if (virtualProvider.hasImperium()) {
//                virtualProvider.executeImperium();
//                return invoker;
//            }
//        }
//
//        Map<Integer, Integer> weightMap = new HashMap<>(invokers.size());
//        List<VirtualProvider> virtualProviderList = new ArrayList<>(invokers.size());
//        for (Invoker<T> invoker : invokers) {
//            map.putIfAbsent(invoker.getUrl().getPort(), new AtomicInteger());
//            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
//            weightMap.put(virtualProvider.getPort(), 0);
//            virtualProviderList.add(virtualProvider);
//            //System.out.println(virtualProvider.getPort() + "'s weight: " + virtualProvider.getWeight(maxWeight) + " num: " + map.get(virtualProvider.getPort()).get());
//        }
//
//        virtualProviderList.sort(Comparator.comparingDouble(VirtualProvider::getRTWeight));
//        int len = Math.min(RTWeightArray.length, virtualProviderList.size());
//        for (int i = 0; i < len; ++i) {
//            int port = virtualProviderList.get(i).getPort();
//            weightMap.put(port, weightMap.get(port) + RTWeightArray[i]);
//        }
//
//        virtualProviderList.sort(Comparator.comparingDouble(VirtualProvider::getConcurrent));
//        for (int i = 0; i < len; ++i) {
//            int port = virtualProviderList.get(i).getPort();
//            weightMap.put(port, weightMap.get(port) + concurrentWeightArray[i]);
//        }
//
////        virtualProviderList.sort(Comparator.comparingDouble(VirtualProvider::getRandomWeight).reversed());
////        for (int i = 0; i < RandomWeightArray.length; ++i) {
////            int port = virtualProviderList.get(i).getPort();
////            weightMap.put(port, weightMap.get(port) + RandomWeightArray[i]);
////        }
////
////        virtualProviderList.sort(Comparator.comparingDouble(VirtualProvider::getThreadFactor).reversed());
////        for (int i = 0; i < ThreadWeightArray.length; ++i) {
////            int port = virtualProviderList.get(i).getPort();
////            weightMap.put(port, weightMap.get(port) + ThreadWeightArray[i]);
////        }
//
//
//        int selectedPort = RoundRobinProcessor.select(weightMap);
//        for (Invoker<T> invoker : invokers) {
//            if (invoker.getUrl().getPort() == selectedPort) return invoker;
//        }
//
//        throw new RpcException("there is no available provider");

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
