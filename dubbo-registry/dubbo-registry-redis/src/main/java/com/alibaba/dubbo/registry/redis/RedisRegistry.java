/*
 * Copyright 1999-2012 Alibaba Group.
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
package com.alibaba.dubbo.registry.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.dubbo.rpc.RpcException;

/**
 * RedisRegistry
 * 
 * @author william.liangf
 */
public class RedisRegistry extends FailbackRegistry {

    private static final Logger logger = LoggerFactory.getLogger(RedisRegistry.class);

    private static final int DEFAULT_REDIS_PORT = 6379;

    private final static String DEFAULT_ROOT = "dubbo";

    // 超时检查的线程池，ScheduledExecutorService
    private final ScheduledExecutorService expireExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("DubboRegistryExpireTimer", true));

    private final ScheduledFuture<?> expireFuture;
    
    private final String root;

    /**
     * 在不同的线程中使用相同的Jedis实例会发生奇怪的错误。但是创建太多的实现也不好因为这意味着会建立很多sokcet连接，
     * 也会导致奇怪的错误发生。单一Jedis实例不是线程安全的。为了避免这些问题，可以使用JedisPool, JedisPool是一个线程安全的网络连接池。
     * 可以用JedisPool创建一些可靠Jedis实例，可以从池中拿到Jedis的实例。这种方式可以解决那些问题并且会实现高效的性能.
     */
    private final Map<String, JedisPool> jedisPools = new ConcurrentHashMap<String, JedisPool>();

    private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<String, Notifier>();
    
    private final int reconnectPeriod;

    private final int expirePeriod;
    
    private volatile boolean admin = false;
    
    private boolean replicate;

    public RedisRegistry(URL url) {
        super(url);
        if (url.isAnyHost()) {
    		throw new IllegalStateException("registry address == null");
    	}

        // 解析url，从url中获取相关连接redis的配置参数
        GenericObjectPool.Config config = new GenericObjectPool.Config();
        config.testOnBorrow = url.getParameter("test.on.borrow", true);
        config.testOnReturn = url.getParameter("test.on.return", false);
        config.testWhileIdle = url.getParameter("test.while.idle", false);
        if (url.getParameter("max.idle", 0) > 0)
            config.maxIdle = url.getParameter("max.idle", 0);
        if (url.getParameter("min.idle", 0) > 0)
            config.minIdle = url.getParameter("min.idle", 0);
        if (url.getParameter("max.active", 0) > 0)
            config.maxActive = url.getParameter("max.active", 0);
        if (url.getParameter("max.wait", url.getParameter("timeout", 0)) > 0)
            config.maxWait = url.getParameter("max.wait", url.getParameter("timeout", 0));
        if (url.getParameter("num.tests.per.eviction.run", 0) > 0)
            config.numTestsPerEvictionRun = url.getParameter("num.tests.per.eviction.run", 0);
        if (url.getParameter("time.between.eviction.runs.millis", 0) > 0)
            config.timeBetweenEvictionRunsMillis = url.getParameter("time.between.eviction.runs.millis", 0);
        if (url.getParameter("min.evictable.idle.time.millis", 0) > 0)
            config.minEvictableIdleTimeMillis = url.getParameter("min.evictable.idle.time.millis", 0);
        
        String cluster = url.getParameter("cluster", "failover");
        if (! "failover".equals(cluster) && ! "replicate".equals(cluster)) {
        	throw new IllegalArgumentException("Unsupported redis cluster: " + cluster + ". The redis cluster only supported failover or replicate.");
        }
        replicate = "replicate".equals(cluster);
        
        List<String> addresses = new ArrayList<String>();
        addresses.add(url.getAddress());// 设置url内部的地址，如果配置port，则包含':端口'
        String[] backups = url.getParameter(Constants.BACKUP_KEY, new String[0]);
        if (backups != null && backups.length > 0) {
            addresses.addAll(Arrays.asList(backups));//加备份服务地址
        }
        // 下面对地址信息进行完备，没有port的，设置redis默认端口6379
        for (String address : addresses) {
            int i = address.indexOf(':');
            String host;
            int port;
            if (i > 0) {
                host = address.substring(0, i);
                port = Integer.parseInt(address.substring(i + 1));
            } else {
                host = address;
                port = DEFAULT_REDIS_PORT;
            }

            // 对每一个地址建立redis pool ，然后加映射map关系。配置信息如上设置
            this.jedisPools.put(address, new JedisPool(config, host, port, 
                    url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT)));// 超时时间默认1000秒
        }

        // 重新注册连接的间隔时间
        this.reconnectPeriod = url.getParameter(Constants.REGISTRY_RECONNECT_PERIOD_KEY, Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD);
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);// 获取group参数，默认为dubbo
        if (! group.startsWith(Constants.PATH_SEPARATOR)) { // 如果group没有以‘/分隔符’开始，则加上该前缀
            group = Constants.PATH_SEPARATOR + group;
        }
        if (! group.endsWith(Constants.PATH_SEPARATOR)) {// 如果group没有以‘/分隔符’结束，则加上该后缀
            group = group + Constants.PATH_SEPARATOR;
        }
        this.root = group;
        
        this.expirePeriod = url.getParameter(Constants.SESSION_TIMEOUT_KEY, Constants.DEFAULT_SESSION_TIMEOUT);// session过期时间

        // 起一个线程池，每expirePeriod / 2毫秒就会执行调度任务deferExpired()
        this.expireFuture = expireExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    deferExpired(); // 延长过期时间
                } catch (Throwable t) { // 防御性容错
                    logger.error("Unexpected exception occur at defer expire time, cause: " + t.getMessage(), t);
                }
            }
        }, expirePeriod / 2, expirePeriod / 2, TimeUnit.MILLISECONDS);
    }
    
    private void deferExpired() {
        // 对每一个redis线程池entry，
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                // 从池中获取一个jedis实例
                Jedis jedis = jedisPool.getResource();
                try {
                    for (URL url : new HashSet<URL>(getRegistered())) {// 对每一个注册上的url进行操作
                        if (url.getParameter(Constants.DYNAMIC_KEY, true)) {

                            // 构建一个完整的key，包含协议用户名密码等信息的字符串
                            String key = toCategoryPath(url);
                            // 如果设置hash表key的域url.toFullString()的值设为 “System.currentTimeMillis() + expirePeriod”
                            if (jedis.hset(key, url.toFullString(), String.valueOf(System.currentTimeMillis() + expirePeriod)) == 1) {
                                jedis.publish(key, Constants.REGISTER);// 发布注册服务url
                            }
                        }
                    }
                    if (admin) {
                        // 监控中心负责删除过期脏数据，一般人员不需要去关心
                        clean(jedis);
                    }
                    if (! replicate) {
                    	break;//  如果服务器端已同步数据，只需写入单台机器
                    }
                } finally {
                    // 释放资源
                    jedisPool.returnResource(jedis);
                }
            } catch (Throwable t) {
                logger.warn("Failed to write provider heartbeat to redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
    }
    
    // 监控中心负责删除过期脏数据
    private void clean(Jedis jedis) {
        Set<String> keys = jedis.keys(root + Constants.ANY_VALUE);// 获取“指定group开头”的所有key集合
        if (keys != null && keys.size() > 0) {
            for (String key : keys) {
                Map<String, String> values = jedis.hgetAll(key);// 返回key表下所有相关的域-->值
                if (values != null && values.size() > 0) {
                    boolean delete = false;
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        URL url = URL.valueOf(entry.getKey());
                        //动态的注册，可以删除，当注销或者时间过期，静态的则一直会存在在注册中心服务器上
                        if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                            long expire = Long.parseLong(entry.getValue());
                            if (expire < now) {
                                jedis.hdel(key, entry.getKey());// 超时过期，删除key下面的某个域
                                delete = true;
                                if (logger.isWarnEnabled()) {
                                    logger.warn("Delete expired key: " + key + " -> value: " + entry.getKey() + ", expire: " + new Date(expire) + ", now: " + new Date(now));
                                }
                            }
                            }
                    }
                    if (delete) {
                        jedis.publish(key, Constants.UNREGISTER);// 删除成功，则发布注销信息
                    }
                }
            }
        }
    }


    // 判断节点是否可提供服务
    public boolean isAvailable() {
        for (JedisPool jedisPool : jedisPools.values()) {
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                	if (jedis.isConnected()) {
                        return true; // 至少需单台机器可用
                    }
                } finally {
                    jedisPool.returnResource(jedis);//释放实例，返还到资源池中
                }
            } catch (Throwable t) {
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            expireFuture.cancel(true);// 取消线程任务的执行
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        try {
            for (Notifier notifier : notifiers.values()) {
                notifier.shutdown();// 销毁notifier线程
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        // 销毁线程池
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                jedisPool.destroy();
            } catch (Throwable t) {
                logger.warn("Failed to destroy the redis registry client. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void doRegister(URL url) { //redis注册
        String key = toCategoryPath(url);
        String value = url.toFullString();
        String expire = String.valueOf(System.currentTimeMillis() + expirePeriod);
        boolean success = false;
        RpcException exception = null;
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    jedis.hset(key, value, expire);
                    jedis.publish(key, Constants.REGISTER);//发布注册消息
                    success = true;
                    if (! replicate) {
                    	break; //  如果服务器端已同步数据，只需写入单台机器
                    }
                } finally {
                    jedisPool.returnResource(jedis);
                }
            } catch (Throwable t) {
                exception = new RpcException("Failed to register service to redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        if (exception != null) {
            if (success) {
                logger.warn(exception.getMessage(), exception);
            } else {
                throw exception;
            }
        }
    }

    @Override
    public void doUnregister(URL url) {//注销
        String key = toCategoryPath(url);
        String value = url.toFullString();
        RpcException exception = null;
        boolean success = false;
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    jedis.hdel(key, value);//删除field value
                    jedis.publish(key, Constants.UNREGISTER);//发布注销消息
                    success = true;
                    if (! replicate) {
                    	break; //  如果服务器端已同步数据，只需写入单台机器
                    }
                } finally {
                    jedisPool.returnResource(jedis);
                }
            } catch (Throwable t) {
                exception = new RpcException("Failed to unregister service to redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        if (exception != null) {
            if (success) {
                logger.warn(exception.getMessage(), exception);
            } else {
                throw exception;
            }
        }
    }
    
    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {// 订阅服务。订阅符合条件的已注册数据，当有注册数据变更时自动推送.
        String service = toServicePath(url);
        Notifier notifier = notifiers.get(service);
        if (notifier == null) {
            Notifier newNotifier = new Notifier(service);
            notifiers.putIfAbsent(service, newNotifier);
            notifier = notifiers.get(service);
            if (notifier == newNotifier) {
                notifier.start();
            }
        }
        boolean success = false;
        RpcException exception = null;
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    if (service.endsWith(Constants.ANY_VALUE)) {
                        admin = true;
                        Set<String> keys = jedis.keys(service);//找到redis中keys，满足pattern
                        if (keys != null && keys.size() > 0) {
                            Map<String, Set<String>> serviceKeys = new HashMap<String, Set<String>>();
                            for (String key : keys) {
                                String serviceKey = toServicePath(key);
                                Set<String> sk = serviceKeys.get(serviceKey);
                                if (sk == null) {
                                    sk = new HashSet<String>();
                                    serviceKeys.put(serviceKey, sk);
                                }
                                sk.add(key);
                            }
                            // 对所有匹配的key都处理
                            for (Set<String> sk : serviceKeys.values()) {
                                doNotify(jedis, sk, url, Arrays.asList(listener));
                            }
                        }
                    } else {
                        // 明确指定了key
                        doNotify(jedis, jedis.keys(service + Constants.PATH_SEPARATOR + Constants.ANY_VALUE), url, Arrays.asList(listener));
                    }
                    success = true;
                    break; // 只需读一个服务器的数据。找到注册中心的一个服务器，注册上去就好了
                } finally {
                    jedisPool.returnResource(jedis);
                }
            } catch(Throwable t) { // 当出现异常的时候，会尝试下一个服务器，指导最后都没有了，才会抛出异常，否则只会打印warn日志
                exception = new RpcException("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        if (exception != null) {
            if (success) {
                logger.warn(exception.getMessage(), exception);
            } else {
                throw exception;
            }
        }
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
    }

    private void doNotify(Jedis jedis, String key) {
        for (Map.Entry<URL, Set<NotifyListener>> entry : new HashMap<URL, Set<NotifyListener>>(getSubscribed()).entrySet()) {// 获取所有需要注册的服务
            doNotify(jedis, Arrays.asList(key), entry.getKey(), new HashSet<NotifyListener>(entry.getValue()));
        }
    }

    /**
     * consumer://10.86.32.73/com.qunar.hotel.qta.order.core.api.service.PayFlowService?
     * application=qta-notify&category=consumers&check=false&connected=true&dubbo=3.0.5&
     * interface=com.qunar.hotel.qta.order.core.api.service.PayFlowService&
     * methods=goPay,queryMainPay,getFailedPayCenterOrderActionRecorders,getSectionName&
     * organization=qta&owner=yongze.li&pid=43879&retries=3&revision=1.3.10&side=consumer&timeout=40000×tamp=1414742317429&version=1.0
     *
     * @param jedis
     * @param keys
     * @param url
     * @param listeners
     */
    private void doNotify(Jedis jedis, Collection<String> keys, URL url, Collection<NotifyListener> listeners) {
        if (keys == null || keys.size() == 0
                || listeners == null || listeners.size() == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        List<URL> result = new ArrayList<URL>();
        List<String> categories = Arrays.asList(url.getParameter(Constants.CATEGORY_KEY, new String[0]));//consumers
        String consumerService = url.getServiceInterface();//com.qunar.hotel.qta.order.core.api.service.PayFlowService
        for (String key : keys) {
            if (! Constants.ANY_VALUE.equals(consumerService)) {
                String prvoiderService = toServiceName(key);//com.qunar.hotel.qta.order.core.api.service.PayFlowService
                if (! prvoiderService.equals(consumerService)) {
                    continue;
                }
            }
            String category = toCategoryName(key);//获取方法名
            if (! categories.contains(Constants.ANY_VALUE) && ! categories.contains(category)) {
                continue;
            }
            List<URL> urls = new ArrayList<URL>();
            Map<String, String> values = jedis.hgetAll(key);//获取key下面所有value。key下面是<url和过期时间>
            if (values != null && values.size() > 0) {
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    URL u = URL.valueOf(entry.getKey());
                    if (! u.getParameter(Constants.DYNAMIC_KEY, true)
                            || Long.parseLong(entry.getValue()) >= now) {//未过期或者静态注册
                        if (UrlUtils.isMatch(url, u)) {//匹配消费者和提供者
                            urls.add(u);
                        }
                    }
                }
            }
            if (urls.isEmpty()) {
                urls.add(url.setProtocol(Constants.EMPTY_PROTOCOL)
                        .setAddress(Constants.ANYHOST_VALUE)
                        .setPath(toServiceName(key))
                        .addParameter(Constants.CATEGORY_KEY, category));
            }
            result.addAll(urls);
            if (logger.isInfoEnabled()) {
                logger.info("redis notify: " + key + " = " + urls);
            }
        }
        if ( result.size() == 0) {
            return;
        }
        for (NotifyListener listener : listeners) {
            notify(url, listener, result);// 服务变更是触发通知
        }
    }

    private String toServiceName(String categoryPath) {
        String servicePath = toServicePath(categoryPath);
        return servicePath.startsWith(root) ? servicePath.substring(root.length()) : servicePath;//不包含组名
    }

    private String toCategoryName(String categoryPath) {
        int i = categoryPath.lastIndexOf(Constants.PATH_SEPARATOR);
        return i > 0 ? categoryPath.substring(i + 1) : categoryPath;
    }

    private String toServicePath(String categoryPath) {
        int i;
        if (categoryPath.startsWith(root)) {
            i = categoryPath.indexOf(Constants.PATH_SEPARATOR, root.length());
        } else {
            i = categoryPath.indexOf(Constants.PATH_SEPARATOR);
        }
        return i > 0 ? categoryPath.substring(0, i) : categoryPath;
    }

    private String toServicePath(URL url) {
        return root + url.getServiceInterface();//group+路径path
    }

    private String toCategoryPath(URL url) {
        return toServicePath(url) + Constants.PATH_SEPARATOR + url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
    }

    // 通知订阅
    private class NotifySub extends JedisPubSub {
        
        private final JedisPool jedisPool;

        public NotifySub(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
        }

        @Override
        public void onMessage(String key, String msg) {// 处理该key下的消息，对于注册和注销消息进行处理
            if (logger.isInfoEnabled()) {
                logger.info("redis event: " + key + " = " + msg);
            }
            if (msg.equals(Constants.REGISTER) 
                    || msg.equals(Constants.UNREGISTER)) {
                try {
                    Jedis jedis = jedisPool.getResource();
                    try {
                        doNotify(jedis, key);
                    } finally {
                        jedisPool.returnResource(jedis);
                    }
                } catch (Throwable t) { // TODO 通知失败没有恢复机制保障
                    logger.error(t.getMessage(), t);
                }
            }
        }

        @Override
        public void onPMessage(String pattern, String key, String msg) {
            onMessage(key, msg);
        }

        @Override
        public void onSubscribe(String key, int num) {
        }

        @Override
        public void onPSubscribe(String pattern, int num) {
        }

        @Override
        public void onUnsubscribe(String key, int num) {
        }

        @Override
        public void onPUnsubscribe(String pattern, int num) {
        }

    }

    private class Notifier extends Thread {

        private final String service;

        private volatile Jedis jedis;

        private volatile boolean first = true;
        
        private volatile boolean running = true;
        
        private final AtomicInteger connectSkip = new AtomicInteger();

        private final AtomicInteger connectSkiped = new AtomicInteger();

        private final Random random = new Random();
        
        private volatile int connectRandom;

        private void resetSkip() {
            connectSkip.set(0);
            connectSkiped.set(0);
            connectRandom = 0;
        }
        
        private boolean isSkip() {
            int skip = connectSkip.get(); // 跳过次数增长
            if (skip >= 10) { // 如果跳过次数增长超过10，取随机数
                if (connectRandom == 0) {
                    connectRandom = random.nextInt(10);
                }
                skip = 10 + connectRandom;
            }
            if (connectSkiped.getAndIncrement() < skip) { // 检查跳过次数
                return true;
            }
            connectSkip.incrementAndGet();
            connectSkiped.set(0);
            connectRandom = 0;
            return false;
        }
        
        public Notifier(String service) {
            super.setDaemon(true);
            super.setName("DubboRedisSubscribe");
            this.service = service;
        }
        
        @Override
        public void run() {
            while (running) {
                try {
                    if (! isSkip()) {
                        try {
                            for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
                                JedisPool jedisPool = entry.getValue();
                                try {
                                    jedis = jedisPool.getResource();
                                    try {
                                        if (service.endsWith(Constants.ANY_VALUE)) {
                                            if (! first) {
                                                first = false;
                                                Set<String> keys = jedis.keys(service);
                                                if (keys != null && keys.size() > 0) {
                                                    for (String s : keys) {
                                                        doNotify(jedis, s);
                                                    }
                                                }
                                                resetSkip();
                                            }
                                            jedis.psubscribe(new NotifySub(jedisPool), service); // 阻塞
                                        } else {
                                            if (! first) {
                                                first = false;
                                                doNotify(jedis, service);
                                                resetSkip();
                                            }
                                            jedis.psubscribe(new NotifySub(jedisPool), service + Constants.PATH_SEPARATOR + Constants.ANY_VALUE); // 阻塞
                                        }
                                        break;
                                    } finally {
                                        jedisPool.returnBrokenResource(jedis);
                                    }
                                } catch (Throwable t) { // 重试另一台
                                    logger.warn("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
                                    // 如果在单台redis的情况下，需要休息一会，避免空转占用过多cpu资源
                                    sleep(reconnectPeriod);
                                }
                            }
                        } catch (Throwable t) {
                            logger.error(t.getMessage(), t);
                            sleep(reconnectPeriod);
                        }
                    }
                } catch (Throwable t) {
                    logger.error(t.getMessage(), t);
                }
            }
        }
        
        public void shutdown() {
            try {
                running = false;
                jedis.disconnect();
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }
        
    }

}
