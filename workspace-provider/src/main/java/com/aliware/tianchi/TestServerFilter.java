package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.rpc.*;
import oshi.SystemInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    private static final SystemInfo SYSTEM_INFO;

    private static final int INIT_TOTAL_THREAD_COUNT;

    private static final Logger LOGGER = LoggerFactory.getLogger(TestServerFilter.class);

    private final AtomicInteger concurrent = new AtomicInteger();

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(64, new NamedInternalThreadFactory("timeout-timer", true));

    static {
        SYSTEM_INFO = new SystemInfo();
        INIT_TOTAL_THREAD_COUNT = SYSTEM_INFO.getOperatingSystem().getThreadCount();
    }

    private static Map<Integer, ConcurrentSkipListSet<Long>> latencyMap = new HashMap<>();


    private static void recordLatency(int port, long latency) {
        latencyMap.putIfAbsent(port, new ConcurrentSkipListSet<>());
        latencyMap.get(port).add(latency);
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //int port = invoker.getUrl().getPort();
        concurrent.incrementAndGet();
        Thread thread = Thread.currentThread();
        scheduledExecutorService.schedule(thread::interrupt, 10, TimeUnit.MILLISECONDS);
        //long startTime = System.currentTimeMillis();
        Result result = invoker.invoke(invocation);
        //long costTime = System.currentTimeMillis() - startTime;
        //recordLatency(port, costTime);
        //System.out.println("concurrent: " + concurrent.get() + " cost time: " + costTime);
        return result;
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        concurrent.decrementAndGet();
        ExecutorRepository executorRepository = ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) executorRepository.getExecutor(invoker.getUrl());
        int maxThreadCount = executor.getMaximumPoolSize();
        int totalThreadCount = SYSTEM_INFO.getOperatingSystem().getThreadCount();
        double threadFactor = (double) maxThreadCount / Math.max(1, totalThreadCount - INIT_TOTAL_THREAD_COUNT);
        //System.out.println("concurrent: " + concurrent.get());
        //LOGGER.info("max: " + maxThreadCount + " total: " + totalThreadCount + " init: " + INIT_TOTAL_THREAD_COUNT + " factor: " + threadFactor);
        //System.out.println("max: " + maxThreadCount + " bucket remain: " + BUCKET.availablePermits() + " init: " + INIT_TOTAL_THREAD_COUNT + " factor: " + threadFactor);
        //System.out.println(" factor: " + threadFactor);
        appResponse.setAttachment(AttachmentKey.CONCURRENT, String.valueOf(concurrent.get()));
        appResponse.setAttachment(AttachmentKey.THREAD_FACTOR, String.valueOf(threadFactor));
        appResponse.setAttachment(AttachmentKey.INVOKE_ID, invocation.getAttachment(AttachmentKey.INVOKE_ID));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        concurrent.decrementAndGet();
    }
}
