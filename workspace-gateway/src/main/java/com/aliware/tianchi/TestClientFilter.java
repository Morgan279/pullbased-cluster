package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.aliware.tianchi.entity.Supervisor;
import com.aliware.tianchi.entity.VirtualProvider;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
        if (virtualProvider.currentLimiter.get() < 1) {
            //System.out.println("work request exceeds limit");
            throw new RpcException("work request exceeds limit");
        }
        virtualProvider.currentLimiter.decrementAndGet();
        //选址后记录RTT
        long startTime = System.currentTimeMillis();
        return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
//            if (t == null) {
//                //System.out.println("recordLatency: " + port + "  " + (System.currentTimeMillis() - startTime) + " weight: " + Supervisor.getVirtualProvider(port).getWeight() + " remain: " + virtualProvider.currentLimiter.get());
//                virtualProvider.recordLatency(System.currentTimeMillis() - startTime);
//            }
            virtualProvider.recordLatency(System.currentTimeMillis() - startTime);
        });
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
        int concurrent = Integer.parseInt(appResponse.getAttachment(AttachmentKey.CONCURRENT));
        virtualProvider.setConcurrent(Integer.parseInt(appResponse.getAttachment(AttachmentKey.CONCURRENT)));
        //       System.out.println("concurrent: " + concurrent + "concurrent: " + virtualProvider.getConcurrent());
        virtualProvider.setThreadFactor(Double.parseDouble(appResponse.getAttachment(AttachmentKey.THREAD_FACTOR)));
        virtualProvider.currentLimiter.incrementAndGet();
        //System.out.println(invoker.getUrl().getPort() + " thread: " + appResponse.getAttachment(AttachmentKey.THREAD_FACTOR));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        int port = invoker.getUrl().getPort();
        VirtualProvider virtualProvider = Supervisor.getVirtualProvider(port);
//        if (t.getMessage().contains("org.apache.dubbo.remoting.TimeoutException")) {
//            Supervisor.getVirtualProvider(port).recordTimeoutRequestId(Long.parseLong(invocation.getAttachment(AttachmentKey.INVOKE_ID)));
//        } else
        if (t.getMessage().contains("thread pool is exhausted")) {
            virtualProvider.currentLimiter.set(0);
        }
        virtualProvider.currentLimiter.incrementAndGet();
        //System.out.println("TestClientFilter error: " + t.getMessage());
        //t.printStackTrace();
    }
}
