package com.aliware.tianchi;

import com.aliware.tianchi.constant.ProviderInfo;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.rpc.*;
import oshi.SystemInfo;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    private static final int REQUEST_LIMIT = 5000;

    private static final SystemInfo SYSTEM_INFO;

    private static final int INIT_TOTAL_THREAD_COUNT;

    private static final Semaphore BUCKET;

    static {
        SYSTEM_INFO = new SystemInfo();
        INIT_TOTAL_THREAD_COUNT = SYSTEM_INFO.getOperatingSystem().getThreadCount();
        int physicalProcessorCount = SYSTEM_INFO.getHardware().getProcessor().getPhysicalProcessorCount();
        BUCKET = new Semaphore(physicalProcessorCount * REQUEST_LIMIT);
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        try {
//            if (!BUCKET.tryAcquire()) {
//                throw new RpcException("work request exceeds limit");
//            }
            //BUCKET.acquire();
            return invoker.invoke(invocation);
        }
//        catch (InterruptedException interruptedException) {
//            Thread.currentThread().interrupt();
//        }
        catch (Exception e) {
            throw e;
        } finally {
            //BUCKET.release();
        }
//        throw new IllegalStateException("Unexpected exception");
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        ExecutorRepository executorRepository = ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) executorRepository.getExecutor(invoker.getUrl());
        int maxThreadCount = executor.getMaximumPoolSize();
        int totalThreadCount = SYSTEM_INFO.getOperatingSystem().getThreadCount();
        double threadFactor = (double) maxThreadCount / Math.max(1, totalThreadCount - INIT_TOTAL_THREAD_COUNT);
        System.out.println("max: " + maxThreadCount + " bucket remain: " + BUCKET.availablePermits() + " init: " + INIT_TOTAL_THREAD_COUNT + " factor: " + threadFactor);
        appResponse.setAttachment(ProviderInfo.THREAD_FACTOR, String.valueOf(threadFactor));
        appResponse.setAttachment("id", invocation.getAttachment("id"));
        appResponse.setAttachment("active", String.valueOf(SYSTEM_INFO.getOperatingSystem().getThreadCount()));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
