package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {

    AtomicInteger successNum = new AtomicInteger();

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(8, new NamedInternalThreadFactory("record-timer", true));


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
        if (virtualProvider.tryRequireConcurrent()) {
            //选址后记录RTT
            virtualProvider.requireConcurrent();
            long startTime = System.currentTimeMillis();
            return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
                long latency = System.currentTimeMillis() - startTime;
                if (t == null && latency < 5000) {
//                    System.out.println("recordLatency: " + port + "  " + latency + " success: " + successNum.incrementAndGet());
                    virtualProvider.recordLatency(latency);
                    virtualProvider.releaseConcurrent();
//                    System.out.println("P99: " + virtualProvider.p99Latency.peek());
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
        virtualProvider.setConcurrent(Integer.parseInt(appResponse.getAttachment(AttachmentKey.CONCURRENT)));
        //       System.out.println("concurrent: " + concurrent + "concurrent: " + virtualProvider.getConcurrent());
        virtualProvider.setThreadFactor(Double.parseDouble(appResponse.getAttachment(AttachmentKey.THREAD_FACTOR)));

        //System.out.println(invoker.getUrl().getPort() + " thread: " + appResponse.getAttachment(AttachmentKey.THREAD_FACTOR));
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
        if (!t.getMessage().contains("work request exceeds limit")) {
            virtualProvider.releaseConcurrent();
        }

        if (!t.getMessage().contains("force timeout") && !t.getMessage().contains("Unexpected exception")) {
            scheduledExecutorService.execute(virtualProvider::recordError);
        }
//        else if (t.getMessage().contains("thread pool is exhausted")) {
//            virtualProvider.currentLimiter.set(virtualProvider.currentLimiter.get() - 100);
//        }
        //System.out.println("TestClientFilter error: " + t.getMessage());
    }
}
