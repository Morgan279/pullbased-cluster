package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.processor.TimeoutProcessor;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 集群实现
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的集群调度算法
 */
public class UserClusterInvoker<T> extends AbstractClusterInvoker<T> {

    //为了compare性能 业务情况下应使用UUID
    private static final AtomicLong counter = new AtomicLong();

    private final TimeoutProcessor<T> timeoutProcessor;

    public UserClusterInvoker(Directory<T> directory) {
        super(directory);
        this.timeoutProcessor = new TimeoutProcessor<>();
        for (Invoker<T> invoker : directory.getAllInvokers()) {
//            for (String key : invoker.getUrl().getParameters().keySet()) {
//                System.out.println("key: " + key + " value: " + invoker.getUrl().getParameter(key));
//            }
            Supervisor.registerProvider(invoker);
        }
    }


    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        Invoker<T> selectedInvoker = loadbalance.select(invokers, getUrl(), invocation);
        Result result = selectedInvoker.invoke(invocation);
        long invokeId = counter.getAndIncrement();
        invocation.setAttachment(AttachmentKey.INVOKE_ID, String.valueOf(invokeId));
        timeoutProcessor.addFuture(RpcContext.getServerContext().getFuture(), selectedInvoker.getUrl().getPort(), invokeId);
        return result;
    }
}
