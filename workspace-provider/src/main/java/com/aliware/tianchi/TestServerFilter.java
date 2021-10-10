package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestServerFilter.class);

    private final AtomicInteger concurrency = new AtomicInteger(0);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int bound = Integer.parseInt(invocation.getAttachment(AttachmentKey.CONCURRENT_BOUND));
        if (concurrency.get() > bound) {
            throw new RpcException();
        }
        concurrency.incrementAndGet();
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        appResponse.setAttachment(AttachmentKey.CONCURRENT, String.valueOf(concurrency.decrementAndGet()));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        
    }
}
