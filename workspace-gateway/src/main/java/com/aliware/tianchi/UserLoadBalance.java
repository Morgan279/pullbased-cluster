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

import java.util.List;

/**
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        WorkRequest request;
//        if ((request = ProviderRecorder.select()) == null) {
//            //LOGGER.info("random select");
//            return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//        } else {
//            //LOGGER.info("specific select");
//            return invokers.stream()
//                    .filter(invoker -> invoker.getUrl().getPort() == request.getPort())
//                    .findFirst()
//                    .orElseThrow(() -> new IllegalArgumentException("unknown invoker"));
//        }

        double maxWeight = -1;
        Invoker<T> selectedInvoker = null;

        for (Invoker<T> invoker : invokers) {
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            if (Supervisor.isProviderAvailable(virtualProvider)) {
                LOGGER.info(invoker.getUrl().getPort() + ": weight: " + virtualProvider.getWeight());
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
    }

}
