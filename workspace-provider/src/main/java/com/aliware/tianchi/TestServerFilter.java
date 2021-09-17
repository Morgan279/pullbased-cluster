package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(32, new NamedInternalThreadFactory("timeout-timer", true));

    private final static Logger logger = LoggerFactory.getLogger(TestServerFilter.class);


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int latencyThreshold = Integer.parseInt(invocation.getAttachment(AttachmentKey.LATENCY_THRESHOLD));
        //       concurrent.incrementAndGet();
        //       logger.info("latencyThreshold: {} concurrent: {} ", latencyThreshold, concurrent.get());
        Thread thread = Thread.currentThread();
        scheduledExecutorService.schedule(thread::interrupt, latencyThreshold, TimeUnit.MILLISECONDS);
        //long startTime = System.nanoTime();
        Result result = invoker.invoke(invocation);
        //long costTime = System.nanoTime() - startTime;
        return result;
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
//        ExecutorRepository executorRepository = ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
//        ThreadPoolExecutor executor = (ThreadPoolExecutor) executorRepository.getExecutor(invoker.getUrl());
//        final int maxThreadCount = executor.getMaximumPoolSize();
//        final int remainThreadCount = Math.max(maxThreadCount + (int)Math.sqrt(maxThreadCount) - concurrent.get(), 0);
        //logger.info("remain thread: {}", remainThreadCount);
        //logger.info("concurrent: {}", concurrent.get());
//        int totalThreadCount = SYSTEM_INFO.getOperatingSystem().getThreadCount();
//        double threadFactor = (double) maxThreadCount / Math.max(1, totalThreadCount - INIT_TOTAL_THREAD_COUNT);
        //System.out.println("concurrent: " + concurrent.get());
        //LOGGER.info("max: " + maxThreadCount + " total: " + totalThreadCount + " init: " + INIT_TOTAL_THREAD_COUNT + " factor: " + threadFactor);
//        appResponse.setAttachment(AttachmentKey.CONCURRENT, String.valueOf(concurrent.decrementAndGet()));
//        appResponse.setAttachment(AttachmentKey.REMAIN_THREAD, String.valueOf(remainThreadCount));
        //appResponse.setAttachment(AttachmentKey.THREAD_FACTOR, String.valueOf(threadFactor));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
//        logger.error("err: {}", t.getMessage());
//        concurrent.decrementAndGet();
    }
}
