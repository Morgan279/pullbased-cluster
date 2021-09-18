package com.aliware.tianchi;

import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.processor.TimeoutProcessor;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;

import java.util.List;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private final TimeoutProcessor<T> timeoutProcessor;

    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
        this.timeoutProcessor = new TimeoutProcessor<>();
        for (Invoker<T> invoker : directory.getAllInvokers()) {
            Supervisor.registerProvider(invoker);
        }
    }


    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        Invoker<T> selectedInvoker = loadbalance.select(invokers, getUrl(), invocation);
        Result result = selectedInvoker.invoke(invocation);
        timeoutProcessor.addFuture(RpcContext.getClientAttachment().getFuture(), selectedInvoker.getUrl().getPort());
        return result;
    }
}
