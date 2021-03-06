package com.aliware.tianchi;

import com.aliware.tianchi.processor.RoundRobinProcessor;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private final static Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    private final AtomicInteger ROUND_COUNTER = new AtomicInteger(0);

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        while (true) {
//            Invoker<T> invoker = invokers.get(ROUND_COUNTER.getAndIncrement() % invokers.size());
//            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
//            if (virtualProvider.remain.get() > 0) {
//                virtualProvider.remain.decrementAndGet();
//                return invoker;
//            }
//            Thread.yield();
//        }
//        for (Invoker<T> invoker : invokers) {
//            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
//            if (virtualProvider.privilege.get() > 0) {
//                virtualProvider.privilege.decrementAndGet();
//                return invoker;
//            }
//        }
//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//        WorkLoad workLoad;
//
//        while ((workLoad = Supervisor.workLoads.pollFirst()) == null) {
//            Thread.yield();
//        }
//        for (Invoker<T> invoker : invokers) {
//            if (invoker.getUrl().getPort() == workLoad.port) {
//                return invoker;
//            }
//        }
//        throw new RpcException("no available provider");
//        while (true) {
////            Invoker<T> selected = invokers.get(ROUND_COUNTER.getAndIncrement() % invokers.size());
//            Invoker<T> selected = selectMinWaitingInvoker(invokers);
//            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(selected.getUrl().getPort());
//            // LOGGER.info("error ratio: {}", virtualProvider.getErrorRatio());
//            if (ThreadLocalRandom.current().nextDouble() > Math.sqrt(virtualProvider.getErrorRatio())) {
//                return selected;
//            }
//        }
//        return invokers.get(ROUND_COUNTER.getAndIncrement() % invokers.size());
        return RoundRobinProcessor.selectMinWaitingInvoker(invokers);
//        return RoundRobinProcessor.selectMinWaitingInvoker(invokers);
    }


}
