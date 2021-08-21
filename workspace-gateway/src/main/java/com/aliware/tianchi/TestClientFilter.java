package com.aliware.tianchi;

import com.aliware.tianchi.constant.ProviderInfo;
import com.aliware.tianchi.entity.Supervisor;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.TimeoutException;
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
        //选址后记录RTT
        long startTime = System.currentTimeMillis();
        return invoker.invoke(invocation).whenCompleteWithContext((r, t) -> {
            if (t == null) {
                int port = invoker.getUrl().getPort();
                System.out.println("recordLatency: " + port + "  " + (System.currentTimeMillis() - startTime) + " active: " + r.getAttachment("active"));
                ProviderRecorder.recordLatency(port, System.currentTimeMillis() - startTime);
                Supervisor.recordLatency(port, System.currentTimeMillis() - startTime);
            }
        });
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        System.out.println(invoker.getUrl().getPort() + " thread: " + appResponse.getAttachment(ProviderInfo.THREAD_FACTOR));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (t instanceof TimeoutException) {
            TimeoutException timeoutException = (TimeoutException) t;
        }
        System.out.println("TestClientFilter error: " + t.getMessage());
        //t.printStackTrace();
    }
}
