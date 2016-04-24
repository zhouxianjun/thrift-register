package com.gary.thriftext.register;

import java.io.Closeable;
import java.net.InetSocketAddress;
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
    List<InetSocketAddress> findServerAddressList(String service, String version);

    /**
     * 选取一个合适的address,可以随机获取等'
     * 内部可以使用合适的算法.
     * @return
     */
    InetSocketAddress selector(String service, String version);
}
