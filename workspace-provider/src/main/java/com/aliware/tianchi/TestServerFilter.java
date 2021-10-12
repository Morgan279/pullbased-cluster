package com.aliware.tianchi;

import com.aliware.tianchi.constant.AttachmentKey;
import com.aliware.tianchi.tool.StopWatch;
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

    private final StopWatch stopWatch = new StopWatch();

    private final StopWatch requestStopWatch = new StopWatch();

    private final AtomicInteger concurrency = new AtomicInteger(0);

//    private final AtomicInteger computed = new AtomicInteger(0);
//
private final AtomicInteger waiting = new AtomicInteger(0);
//
//    private final ConcurrentLimitProcessor clp = new ConcurrentLimitProcessor();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        int bound = Integer.parseInt(invocation.getAttachment(AttachmentKey.CONCURRENT_BOUND));
        waiting.incrementAndGet();
        if (concurrency.get() > bound) {
            throw new RpcException();
        }
        waiting.decrementAndGet();
        concurrency.incrementAndGet();
        return invoker.invoke(invocation);
        //LOGGER.info("request elapsed: {}, RT: {}", requestStopWatch.stop(), System.currentTimeMillis() - Long.parseLong(invocation.getAttachment(AttachmentKey.SEND_TIME)));
//        double requestRT = requestStopWatch.stop();
//        waiting.incrementAndGet();
//        if (concurrency.get() > clp.getBound()) {
//            throw new RpcException();
//        }
//        requestStopWatch.start();
//        waiting.decrementAndGet();
//        concurrency.incrementAndGet();
//        int lastComputed = computed.get();
//        stopWatch.start();
//        Result result = invoker.invoke(invocation);
//        double elapsed = stopWatch.stop();
//        clp.onResponse(requestRT, (computed.incrementAndGet() - lastComputed) / elapsed);
//        return result;
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        //int bound = Integer.parseInt(invocation.getAttachment(AttachmentKey.CONCURRENT_BOUND));
        appResponse.setAttachment(AttachmentKey.CONCURRENT, String.valueOf(concurrency.decrementAndGet()));
        appResponse.setAttachment(AttachmentKey.REMAIN_THREAD, String.valueOf(waiting.get()));
        waiting.set(0);
        //appResponse.setAttachment(AttachmentKey.REMAIN_THREAD, String.valueOf(bound - concurrency.get()));
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        //concurrency.decrementAndGet();
    }
}
