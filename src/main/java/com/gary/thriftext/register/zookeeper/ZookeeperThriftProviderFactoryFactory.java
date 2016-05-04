package com.gary.thriftext.register.zookeeper;

import com.gary.thriftext.register.ThriftServerProviderFactory;
import com.gary.thriftext.register.invoker.Invoker;
import com.gary.thriftext.register.invoker.InvokerFactory;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.springframework.beans.factory.InitializingBean;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author zhouxianjun(Gary)
 * @ClassName:
 * @Description: 使用zookeeper作为"config"中心,使用apache-curator方法库来简化zookeeper开发
 * @date 2016/4/22 11:56
 */
@Slf4j
@NoArgsConstructor
public class ZookeeperThriftProviderFactoryFactory implements InitializingBean, ThriftServerProviderFactory {
    @Setter
    private CuratorFramework zkClient;

    @Setter
    private InvokerFactory invokerFactory;

    private TreeCache treeCache;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private ConcurrentHashMap<String, List<Invoker>> cacheMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, List<String>> addressMap = new ConcurrentHashMap<>();

    private final Object lock = new Object();

    public ZookeeperThriftProviderFactoryFactory(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // 如果zk尚未启动,则启动
        if (zkClient.getState() == CuratorFrameworkState.LATENT) {
            zkClient.start();
        }
        listenTree();
        treeCache.start();
        countDownLatch.await();
    }

    private void listenTree() {
        treeCache = new TreeCache(zkClient, "/");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                TreeCacheEvent.Type type = treeCacheEvent.getType();
                log.debug("zookeeper event:{}", type);
                Map<String, List<String>> temp = new HashMap<>();
                rebuild("/", temp);
                synchronized (lock) {
                    addressMap.clear();
                    //销毁不存在的服务
                    Iterator<Map.Entry<String, List<Invoker>>> iterator = cacheMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, List<Invoker>> entry = iterator.next();
                        if (!temp.containsKey(entry.getKey())) {
                            for (Invoker invoker : entry.getValue()) {
                                log.info("zookeeper unsubscribe {}-{}", entry.getKey(), invoker.getAddress());
                                invoker.destroy();
                            }
                            iterator.remove();
                            continue;
                        }

                        //销毁不存在的地址
                        Iterator<Invoker> it = entry.getValue().iterator();
                        List<String> addressList = temp.get(entry.getKey());
                        while (it.hasNext()) {
                            Invoker invoker = it.next();
                            boolean available = invoker.isAvailable();
                            for (String address : addressList) {
                                if (!address.equals(invoker.getAddress())) {
                                    available = false;
                                    break;
                                }
                            }
                            if (!available) {
                                log.info("zookeeper unsubscribe {}-{}", entry.getKey(), invoker.getAddress());
                                invoker.destroy();
                                it.remove();
                            }
                        }
                    }


                    for (Map.Entry<String, List<String>> entry : temp.entrySet()) {
                        if (!addressMap.containsKey(entry.getKey())) {
                            addressMap.put(entry.getKey(), new ArrayList<String>());
                        }
                        log.info("zookeeper subscribe {}-{}", entry.getKey(), Arrays.toString(entry.getValue().toArray()));
                        addressMap.get(entry.getKey()).addAll(entry.getValue());
                    }
                }
                temp = null;
                if (type == TreeCacheEvent.Type.INITIALIZED)
                    countDownLatch.countDown();
            }

            private void rebuild(String root, Map<String, List<String>> map) {
                Map<String, ChildData> currentChildren = treeCache.getCurrentChildren(root);
                if (currentChildren != null) {
                    for (Map.Entry<String, ChildData> entry : currentChildren.entrySet()) {
                        String path = entry.getValue().getPath();
                        log.info("zookeeper rebuild path:{}", path);
                        String[] array = path.split("/");
                        if (array.length == 4) {
                            String key = array[1] + ":" + array[2];
                            if (!map.containsKey(key))
                                map.put(key, new ArrayList<String>());
                            map.get(key).add(array[3]);
                        }
                        rebuild(path, map);
                    }
                }
            }
        });
    }

    /**
     * 获取所有服务端地址
     * @param service 接口
     * @param version 版本
     * @return
     */
    public List<Invoker> allServerAddressList(String service, String version, Class<?> referenceClass) throws Exception {
        synchronized (lock) {
            String key = service + ":" + version;
            List<String> addressList = addressMap.get(key);
            if (!cacheMap.containsKey(key)) {
                cacheMap.put(key, new ArrayList<Invoker>());
            }
            for (String address : addressList) {
                boolean have = false;
                for (Invoker invoker : cacheMap.get(key)) {
                    if (invoker.getAddress().equals(address)) {
                        have = true;
                        break;
                    }
                }
                if (!have) {
                    cacheMap.get(key).add(invokerFactory.newInvoker(address, referenceClass));
                    break;
                }
            }
            List<Invoker> addresses = cacheMap.get(key);
            return addresses == null ? null : Collections.unmodifiableList(addresses);
        }
    }

    public void close() {
        try {
            treeCache.close();
            for (List<Invoker> invokers : cacheMap.values()) {
                for (Invoker invoker : invokers) {
                    invoker.destroy();
                }
            }
            zkClient.close();
        } catch (Exception e) {
            log.warn("stop TreeCache or zkClient error", e);
        }
    }
}
