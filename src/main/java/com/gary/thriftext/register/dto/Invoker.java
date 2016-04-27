package com.gary.thriftext.register.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description:
 * @date 2016/4/27 9:57
 */
@Data
@ToString
@NoArgsConstructor
public class Invoker {
    private String host;

    private int port;

    private int weight = 100;

    private long startTime = 0L;

    private int warmup = 10 * 60 * 1000;

    private String address;

    public Invoker(String address) {
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
        this.address = address;
    }
}
