package com.gary.thriftext.register;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 注册中心
 * @date 2016/4/22 11:41
 */
public interface ThriftServerRegister {
    /**
     * 发布服务接口
     * @param service 服务接口名称，一个产品中不能重复
     * @param version 服务接口的版本号，默认1.0.0
     * @param address 服务发布的地址和端口
     */
    void register(String service, String version, String address) throws Exception;
}
