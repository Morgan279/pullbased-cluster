package com.aliware.tianchi;

import com.aliware.tianchi.constant.ProviderInfo;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.rpc.*;
import oshi.SystemInfo;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
            return invoker.invoke(invocation);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        ExecutorRepository executorRepository = ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) executorRepository.getExecutor(invoker.getUrl());
        int currentThreadCount = ManagementFactory.getThreadMXBean().getThreadCount();
        int maxThreadCount = executor.getMaximumPoolSize();
        int totalThreadCount = new SystemInfo().getOperatingSystem().getThreadCount();
        System.out.println("max: " + maxThreadCount + " current: " + currentThreadCount + " available: " + executor.getActiveCount());
        appResponse.setAttachment(ProviderInfo.THREAD_FACTOR, String.valueOf((double) maxThreadCount / totalThreadCount));
        appResponse.setAttachment("active", String.valueOf(new SystemInfo().getOperatingSystem().getThreadCount()));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
