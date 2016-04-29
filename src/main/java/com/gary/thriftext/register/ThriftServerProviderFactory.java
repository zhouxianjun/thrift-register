package com.gary.thriftext.register;

import com.gary.thriftext.register.invoker.Invoker;

import java.io.Closeable;
import java.util.List;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: thrift server-service地址提供者,以便构建客户端连接池
 * @date 2016/4/22 11:43
 */
public interface ThriftServerProviderFactory extends Closeable {
    /**
     * 获取所有服务端地址
     * @param service 接口
     * @param version 版本
     * @return
     */
    List<Invoker> allServerAddressList(String service, String version, Class<?> referenceClass) throws Exception;
}
