package com.aliware.tianchi;

import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.processor.TimeoutProcessor;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private final TimeoutProcessor<T> timeoutProcessor;
    private final static Logger LOGGER = LoggerFactory.getLogger(UserClusterInvoker.class);


    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
        this.timeoutProcessor = new TimeoutProcessor<>();
        for (Invoker<T> invoker : directory.getAllInvokers()) {
            Supervisor.registerProvider(invoker);
        }
        LOGGER.info("register {} provider at total", Supervisor.virtualProviderMap.values().size());
    }


    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
//        LOGGER.info("Latency Threshold: {}", Supervisor.getLatencyThreshold());
//        RpcContext.getClientAttachment().setAttachment(CommonConstants.TIMEOUT_KEY, Supervisor.getLatencyThreshold());
        return loadbalance.select(invokers, getUrl(), invocation).invoke(invocation);
//        Invoker<T> selectedInvoker = loadbalance.select(invokers, getUrl(), invocation);
//        Result result = selectedInvoker.invoke(invocation);
//        timeoutProcessor.addFuture(RpcContext.getClientAttachment().getFuture());
//        return result;
    }
}
