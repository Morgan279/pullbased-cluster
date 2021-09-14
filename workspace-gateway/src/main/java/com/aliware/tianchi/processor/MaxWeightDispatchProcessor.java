package com.aliware.tianchi.processor;

import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;

import java.util.List;

public class MaxWeightDispatchProcessor {

    public static <T> Invoker<T> select(List<Invoker<T>> invokers) {
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
    }

}
