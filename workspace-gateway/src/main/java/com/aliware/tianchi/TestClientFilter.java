package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionException;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {

    private final static Logger LOGGER = LoggerFactory.getLogger(UserLoadBalance.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);

        virtualProvider.waiting.incrementAndGet();
        while (virtualProvider.isConcurrentLimited()) {
            Thread.yield();
        }
        virtualProvider.inflight.incrementAndGet();
        virtualProvider.waiting.decrementAndGet();
        int lastComputed = virtualProvider.computed.get();

        RpcContext.getClientAttachment().setAttachment(CommonConstants.TIMEOUT_KEY, virtualProvider.getLatencyThreshold());
        long startTime = System.nanoTime();
        return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
//            virtualProvider.refreshErrorSampling();
//            virtualProvider.assigned.incrementAndGet();
            virtualProvider.inflight.decrementAndGet();
            virtualProvider.computed.incrementAndGet();
//            double RTT = (System.nanoTime() - startTime) / 1e6;
            //virtualProvider.estimateInflight((virtualProvider.comingNum.get() - lastComing - (virtualProvider.computed.get() - lastComputed)));
//            logger.info("RTT: {}", latency / 1e6);
            if (t == null) {
                long latency = System.nanoTime() - startTime;
                virtualProvider.onComputed(latency, lastComputed);
            }
//            else {
//                //virtualProvider.error.incrementAndGet();
//                Supervisor.workLoads.add(new WorkLoad(port, 5000 * ThreadLocalRandom.current().nextDouble()));
//            }
        });

    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
//        LOGGER.info("{}'s concurrency: {}",invoker.getUrl().getPort(),appResponse.getAttachment(AttachmentKey.CONCURRENT));
        virtualProvider.concurrency = Integer.parseInt(appResponse.getAttachment(AttachmentKey.CONCURRENT));
        LOGGER.info("{}'s concurrency ratio: {}",invoker.getUrl().getPort(),virtualProvider.getConcurrencyRatio());
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
//        if (t.getMessage() != null && t.getMessage().contains("thread pool is exhausted")) {
//            logger.warn("exhausted");
//            VirtualProvider virtualProvider = Supervisor.getVirtualProvider(invoker.getUrl().getPort());
//            virtualProvider.switchDrain();
//        }

//        LOGGER.info("t class: {}", t.getClass());
        if (t.getClass().equals(CompletionException.class)) {
            LOGGER.error("TestClientFilter onError: {}", t.getMessage());
        }
    }
}
