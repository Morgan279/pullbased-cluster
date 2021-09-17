package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @SuppressWarnings("StatementWithEmptyBody")
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);

        while (virtualProvider.isConcurrentLimited()) ;

        invocation.setAttachment(AttachmentKey.LATENCY_THRESHOLD, String.valueOf(virtualProvider.getLatencyThreshold()));
        int lastComputed = virtualProvider.computed.get();
        virtualProvider.inflight.incrementAndGet();
        long startTime = System.nanoTime();
        return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
            virtualProvider.inflight.decrementAndGet();
            if (t == null) {
                long latency = System.nanoTime() - startTime;
                virtualProvider.onComputed(latency, lastComputed);
                //logger.info("lastComputed: {} nowComputed: {} diff: {}", lastComputed, virtualProvider.computed.incrementAndGet(),(c));
                //logger.info("latency: {} RTprop: {}", latency / (int) 1e6, (latency - executeElapse) / (int) 1e6);
                //virtualProvider.recordLatency(latency * (int) 1e6);
            }
        });

        //throw new RpcException("Work request exceeds limitation");
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
//        int port = invoker.getUrl().getPort();
//        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
//        int concurrent = Integer.parseInt(appResponse.getAttachment(AttachmentKey.CONCURRENT));
//        virtualProvider.setConcurrent(concurrent);
//        virtualProvider.setRemainThreadCount(Integer.parseInt(appResponse.getAttachment(AttachmentKey.REMAIN_THREAD)));
//        virtualProvider.setThreadFactor(Double.parseDouble(appResponse.getAttachment(AttachmentKey.THREAD_FACTOR)));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
//        int port = invoker.getUrl().getPort();
//        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
//        virtualProvider.computed.incrementAndGet();
//        if (!t.getMessage().contains("force timeout") && !t.getMessage().contains("Unexpected exception")) {
//            virtualProvider.recordError();
//        }
//        else if (t.getMessage().contains("thread pool is exhausted")) {
//            virtualProvider.currentLimiter.set(virtualProvider.currentLimiter.get() - 100);
//        }
        //System.out.println("TestClientFilter error: " + t.getMessage());
    }
}
