package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

    private final AtomicInteger concurrent = new AtomicInteger();

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(256, new NamedInternalThreadFactory("timeout-timer", true));

    private final static Logger logger = LoggerFactory.getLogger(TestServerFilter.class);

    static {
        SYSTEM_INFO = new SystemInfo();
        INIT_TOTAL_THREAD_COUNT = SYSTEM_INFO.getOperatingSystem().getThreadCount();
    }

    private static Map<Integer, List<Long>> latencyMap = new HashMap<>();

    private static volatile long threshold = 50;

    private synchronized static void recordLatency(int port, long latency) {
        latencyMap.putIfAbsent(port, new ArrayList<>(10000));
        List<Long> latencyList = latencyMap.get(port);
        latencyList.add(latency);
        if (latencyList.size() == 10000) {
            latencyList.sort(Long::compare);
            threshold = latencyList.get(30);
            latencyList.clear();
        }
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int port = invoker.getUrl().getPort();
        concurrent.incrementAndGet();
        logger.info("concurrent: " + concurrent.get());
        Thread thread = Thread.currentThread();
        scheduledExecutorService.schedule(thread::interrupt, threshold, TimeUnit.MILLISECONDS);
        long startTime = System.currentTimeMillis();
        Result result = invoker.invoke(invocation);
        long costTime = System.currentTimeMillis() - startTime;
        scheduledExecutorService.execute(() -> recordLatency(port, costTime));
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
