/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gary.thriftext.register.dto;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RpcStatus {

    private static final ConcurrentMap<String, RpcStatus> SERVICE_STATISTICS = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, ConcurrentMap<String, RpcStatus>> METHOD_STATISTICS = new ConcurrentHashMap<>();

    public static RpcStatus getStatus(String address) {
        RpcStatus status = SERVICE_STATISTICS.get(address);
        if (status == null) {
            SERVICE_STATISTICS.putIfAbsent(address, new RpcStatus());
            status = SERVICE_STATISTICS.get(address);
        }
        return status;
    }
    
    public static void removeStatus(String address) {
        SERVICE_STATISTICS.remove(address);
    }
    
    /**
     * 
     * @param methodName
     * @return status
     */
    public static RpcStatus getStatus(String address, String methodName) {
        ConcurrentMap<String, RpcStatus> map = METHOD_STATISTICS.get(address);
        if (map == null) {
            METHOD_STATISTICS.putIfAbsent(address, new ConcurrentHashMap<String, RpcStatus>());
            map = METHOD_STATISTICS.get(address);
        }
        RpcStatus status = map.get(methodName);
        if (status == null) {
            map.putIfAbsent(methodName, new RpcStatus());
            status = map.get(methodName);
        }
        return status;
    }

    /**
     * 
     */
    public static void removeStatus(String address, String methodName) {
        ConcurrentMap<String, RpcStatus> map = METHOD_STATISTICS.get(address);
        if (map != null) {
            map.remove(methodName);
        }
    }

    public static void beginCount(String address, String methodName) {
        beginCount(getStatus(address));
        beginCount(getStatus(address, methodName));
    }
    public static void beginCount(String address) {
        beginCount(getStatus(address));
    }

    private static void beginCount(RpcStatus status) {
        status.active.incrementAndGet();
    }

    /**
     * 
     * @param elapsed
     * @param succeeded
     */
    public static void endCount(String address, String methodName, long elapsed, boolean succeeded) {
        endCount(getStatus(address), elapsed, succeeded);
        endCount(getStatus(address, methodName), elapsed, succeeded);
    }
    public static void endCount(String address, long elapsed, boolean succeeded) {
        endCount(getStatus(address), elapsed, succeeded);
    }

    private static void endCount(RpcStatus status, long elapsed, boolean succeeded) {
        status.active.decrementAndGet();
        status.total.incrementAndGet();
        status.totalElapsed.addAndGet(elapsed);
        if (status.maxElapsed.get() < elapsed) {
            status.maxElapsed.set(elapsed);
        }
        if (succeeded) {
            if (status.succeededMaxElapsed.get() < elapsed) {
                status.succeededMaxElapsed.set(elapsed);
            }
        } else {
            status.failed.incrementAndGet();
            status.failedElapsed.addAndGet(elapsed);
            if (status.failedMaxElapsed.get() < elapsed) {
                status.failedMaxElapsed.set(elapsed);
            }
        }
    }

    private final ConcurrentMap<String, Object> values = new ConcurrentHashMap<String, Object>();

    private final AtomicInteger active = new AtomicInteger();

    private final AtomicLong total = new AtomicLong();

    private final AtomicInteger failed = new AtomicInteger();

    private final AtomicLong totalElapsed = new AtomicLong();

    private final AtomicLong failedElapsed = new AtomicLong();

    private final AtomicLong maxElapsed = new AtomicLong();

    private final AtomicLong failedMaxElapsed = new AtomicLong();

    private final AtomicLong succeededMaxElapsed = new AtomicLong();
    
    private RpcStatus() {}

    /**
     * set value.
     * 
     * @param key
     * @param value
     */
    public void set(String key, Object value) {
        values.put(key, value);
    }

    /**
     * get value.
     * 
     * @param key
     * @return value
     */
    public Object get(String key) {
        return values.get(key);
    }

    /**
     * get active.
     * 
     * @return active
     */
    public int getActive() {
        return active.get();
    }

    /**
     * get total.
     * 
     * @return total
     */
    public long getTotal() {
        return total.longValue();
    }
    
    /**
     * get total elapsed.
     * 
     * @return total elapsed
     */
    public long getTotalElapsed() {
        return totalElapsed.get();
    }

    /**
     * get average elapsed.
     * 
     * @return average elapsed
     */
    public long getAverageElapsed() {
        long total = getTotal();
        if (total == 0) {
            return 0;
        }
        return getTotalElapsed() / total;
    }

    /**
     * get max elapsed.
     * 
     * @return max elapsed
     */
    public long getMaxElapsed() {
        return maxElapsed.get();
    }

    /**
     * get failed.
     * 
     * @return failed
     */
    public int getFailed() {
        return failed.get();
    }

    /**
     * get failed elapsed.
     * 
     * @return failed elapsed
     */
    public long getFailedElapsed() {
        return failedElapsed.get();
    }

    /**
     * get failed average elapsed.
     * 
     * @return failed average elapsed
     */
    public long getFailedAverageElapsed() {
        long failed = getFailed();
        if (failed == 0) {
            return 0;
        }
        return getFailedElapsed() / failed;
    }

    /**
     * get failed max elapsed.
     * 
     * @return failed max elapsed
     */
    public long getFailedMaxElapsed() {
        return failedMaxElapsed.get();
    }

    /**
     * get succeeded.
     * 
     * @return succeeded
     */
    public long getSucceeded() {
        return getTotal() - getFailed();
    }

    /**
     * get succeeded elapsed.
     * 
     * @return succeeded elapsed
     */
    public long getSucceededElapsed() {
        return getTotalElapsed() - getFailedElapsed();
    }

    /**
     * get succeeded average elapsed.
     * 
     * @return succeeded average elapsed
     */
    public long getSucceededAverageElapsed() {
        long succeeded = getSucceeded();
        if (succeeded == 0) {
            return 0;
        }
        return getSucceededElapsed() / succeeded;
    }

    /**
     * get succeeded max elapsed.
     * 
     * @return succeeded max elapsed.
     */
    public long getSucceededMaxElapsed() {
        return succeededMaxElapsed.get();
    }

    /**
     * Calculate average TPS (Transaction per second).
     *
     * @return tps
     */
    public long getAverageTps() {
        if (getTotalElapsed() >= 1000L) {
            return getTotal() / (getTotalElapsed() / 1000L);
        }
        return getTotal();
    }

}