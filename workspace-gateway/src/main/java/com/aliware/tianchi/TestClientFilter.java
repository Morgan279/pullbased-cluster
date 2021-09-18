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

        virtualProvider.waiting.incrementAndGet();
        while (virtualProvider.isConcurrentLimited()) {
            Thread.yield();
        }
        virtualProvider.waiting.decrementAndGet();

        invocation.setAttachment(AttachmentKey.LATENCY_THRESHOLD, String.valueOf(virtualProvider.getLatencyThreshold()));
        int lastComputed = virtualProvider.computed.get();
        virtualProvider.inflight.incrementAndGet();

        long startTime = System.nanoTime();
        return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
            virtualProvider.inflight.decrementAndGet();
            if (t == null) {
                long latency = System.nanoTime() - startTime;
                virtualProvider.onComputed(latency, lastComputed);
            }
        });

    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {

    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }
}
