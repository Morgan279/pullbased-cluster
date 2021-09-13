package com.aliware.tianchi;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 客户端过滤器（选址前）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientClusterFilter implements ClusterFilter, BaseFilter.Listener {

    private static final AtomicInteger counter = new AtomicInteger();

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(512, new NamedInternalThreadFactory("test-timer", true));


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //若没有可用的provider 则在选址前拦截请求
//        if (Supervisor.isOutOfService()) {
//            throw new RpcException("Temporarily out of service");
//        }
        RpcInvocation inv = (RpcInvocation) invocation;
        inv.setInvokeMode(InvokeMode.FUTURE);
        return invoker.invoke(inv);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
