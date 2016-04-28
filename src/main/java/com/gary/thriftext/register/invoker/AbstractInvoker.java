package com.gary.thriftext.register.invoker;

import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/28 14:35
 */
@NoArgsConstructor
public abstract class AbstractInvoker implements Invoker {
    @Getter
    private String host;

    @Getter
    private int port;

    @Getter
    private int weight = 100;

    @Getter
    private long startTime = 0L;

    @Getter
    private int warmup = 10 * 60 * 1000;

    @Getter
    private String address;

    private Class<?> interfaceClass;

    public AbstractInvoker(String address, Class<?> interfaceClass) {
        String[] hostname = address.split(":");
        if (hostname.length >= 3) {
            this.weight = Integer.valueOf(hostname[2]);
        }
        if (hostname.length >= 4) {
            this.startTime = Long.valueOf(hostname[3]);
        }
        if (hostname.length == 5) {
            this.warmup = Integer.valueOf(hostname[4]);
        }
        this.host = hostname[0];
        this.port = Integer.valueOf(hostname[1]);
        this.interfaceClass = interfaceClass;
        this.address = address;
    }

    @Override
    public Class<?> getInterface() {
        return interfaceClass;
    }
}
