package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {

    //private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(8, new NamedInternalThreadFactory("record-timer", true));
    private final static Logger logger = LoggerFactory.getLogger(UserLoadBalance.class);


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
        long now = System.currentTimeMillis();
        if(now - virtualProvider.lastInvokeTime >= virtualProvider.averageRT){
            virtualProvider.lastInvokeTime = now;
            virtualProvider.inflight.set(0);
        }
        if (virtualProvider.inflight.get() < virtualProvider.getConcurrent()) {
            //选址后记录RTT
//            virtualProvider.requireConcurrent();
            long startTime = System.currentTimeMillis();
            invocation.setAttachment(AttachmentKey.LATENCY_THRESHOLD, String.valueOf(virtualProvider.getLatencyThreshold()));
            virtualProvider.inflight.incrementAndGet();
            return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
//                logger.info("inflight: {}", virtualProvider.inflight.decrementAndGet());
                virtualProvider.inflight.decrementAndGet();
                long latency = System.currentTimeMillis() - startTime;
                if (t == null) {
//                    logger.info("recordLatency: " + port + "  " + latency);
                    virtualProvider.recordLatency(latency);
//                    virtualProvider.releaseConcurrent();
                }
            });
        }

        //RpcContext.getClientAttachment().getFuture().cancel(true);
        throw new RpcException("work request exceeds limit");
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
        int concurrent = Integer.parseInt(appResponse.getAttachment(AttachmentKey.CONCURRENT));
        virtualProvider.setConcurrent(concurrent);
        virtualProvider.setRemainThreadCount(Integer.parseInt(appResponse.getAttachment(AttachmentKey.REMAIN_THREAD)));
        virtualProvider.setThreadFactor(Double.parseDouble(appResponse.getAttachment(AttachmentKey.THREAD_FACTOR)));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
        //scheduledExecutorService.schedule(virtualProvider.currentLimiter::incrementAndGet, 5, TimeUnit.MILLISECONDS);
        //virtualProvider.releaseConcurrent();
//        if (t.getMessage().contains("org.apache.dubbo.remoting.TimeoutException")) {
//            Supervisor.getVirtualProvider(port).recordTimeoutRequestId(Long.parseLong(invocation.getAttachment(AttachmentKey.INVOKE_ID)));
//        } else
//        if (!t.getMessage().contains("work request exceeds limit")) {
//            virtualProvider.releaseConcurrent();
//        }

        if (!t.getMessage().contains("force timeout") && !t.getMessage().contains("Unexpected exception")) {
            //scheduledExecutorService.execute(virtualProvider::recordError);
            virtualProvider.recordError();
        }
//        else if (t.getMessage().contains("thread pool is exhausted")) {
//            virtualProvider.currentLimiter.set(virtualProvider.currentLimiter.get() - 100);
//        }
        //System.out.println("TestClientFilter error: " + t.getMessage());
    }
}
