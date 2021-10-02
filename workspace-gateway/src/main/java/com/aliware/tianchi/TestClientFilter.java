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

    private final static Logger logger = LoggerFactory.getLogger(UserLoadBalance.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);

//
        if (virtualProvider.isConcurrentLimited()) {
            throw new RpcException();
        }

        virtualProvider.waiting.incrementAndGet();
        while (!virtualProvider.concurrentLimitProcessor.tokenBucket.canSend()){
            Thread.yield();
        }

//        while (virtualProvider.isConcurrentLimited()) {
//            Thread.yield();
//        }
//        while (virtualProvider.concurrentLimitProcessor.funnel.poll() == null) {
//            Thread.yield();
//        }

        long startTime = System.nanoTime();
        virtualProvider.concurrentLimitProcessor.tokenBucket.send(startTime, virtualProvider.waiting);
        invocation.setAttachment(AttachmentKey.LATENCY_THRESHOLD, String.valueOf(virtualProvider.getLatencyThreshold()));
        int lastComputed = virtualProvider.computed.get();
        virtualProvider.inflight.incrementAndGet();
        return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
//            virtualProvider.refreshErrorSampling();
            virtualProvider.assigned.incrementAndGet();
            virtualProvider.inflight.decrementAndGet();
//            double RTT = (System.nanoTime() - startTime) / 1e6;
            //virtualProvider.estimateInflight((virtualProvider.comingNum.get() - lastComing - (virtualProvider.computed.get() - lastComputed)));
            if (t == null) {
                long latency = System.nanoTime() - startTime;
//                logger.info("RTT: {}", latency / 1e6);
                virtualProvider.computed.incrementAndGet();
//                inflight = Math.max(inflight, virtualProvider.comingNum.get() - lastComing - (virtualProvider.computed.get() - lastComputed));
//
//                logger.info("coming: {}, computed: {}, inflight: {}, rate: {}",
//                        virtualProvider.comingNum.get() - lastComing,
//                        virtualProvider.computed.get() - lastComputed,
//                        inflight,
//                        (virtualProvider.computed.get() - lastComputed) / (latency / 1e6)
//                );
                virtualProvider.onComputed(latency, lastComputed);
            } else {
                virtualProvider.error.incrementAndGet();
            }
        });

    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (t.getMessage().contains("thread pool is exhausted")) {
            //logger.warn("exhausted");
            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
            virtualProvider.switchDrain();
        }
        //logger.error("onError", t);
    }
}
